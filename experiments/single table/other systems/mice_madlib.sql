CREATE OR REPLACE PROCEDURE MICE_madlib(
        input_table_name text,
        output_table_name text,
        continuous_columns text[],
        categorical_columns text[],
        continuous_columns_null text[], 
        categorical_columns_null text[]
) LANGUAGE plpgsql AS $$
    DECLARE
        columns text[];
        _imputed_data float8[];
        mice_iter int := 1;
        _counter int;
        _counter_2 int;
        _query text := 'SELECT ';
        _query_2 text := '';
        _indep_vars text[];
        col text;
        _start_ts timestamptz;
    	_end_ts   timestamptz;
		_int_ts timestamptz;
		tmp_array text[];
    	tmp_array2 text[];
    	tmp_array3 text[];
    	cat_cols_1hot text[];
    	query text;
    	query2 text;
    	start_ts timestamptz;
    	end_ts   timestamptz;
    	col_averages float8[];
    	col_mode int4[];
		indep_vars text[];



    BEGIN
    columns := continuous_columns || categorical_columns || ARRAY['ROW_ID'];
    ---drop table res_linregr_summary, res_linregr    
    SELECT array_agg('AVG(' || x || ')')
    FROM unnest(continuous_columns_null) AS x
    INTO tmp_array;
    
    SELECT array_agg('MODE() WITHIN GROUP (ORDER BY ' || x || ') ')
    FROM unnest(categorical_columns_null) AS x
    INTO tmp_array2;
    
    query := ' SELECT ARRAY[ ' || array_to_string(tmp_array, ', ') || ' ]::float8[]' || 
             ' FROM ( SELECT ' || array_to_string(continuous_columns_null, ', ') || 
                    ' FROM ' || input_table_name || ' LIMIT 500000 ) AS t';
    query2 := ' SELECT ARRAY[ ' || array_to_string(tmp_array2, ', ') || ' ]::int[]' || 
             ' FROM ( SELECT ' || array_to_string(categorical_columns_null, ', ') || 
                    ' FROM ' || input_table_name || ' LIMIT 500000 ) AS t';
    
    RAISE DEBUG '%', query;
    
    start_ts := clock_timestamp();
    
    IF array_length(continuous_columns_null, 1) > 0 THEN
    EXECUTE query INTO col_averages;
    END IF;
    IF array_length(categorical_columns_null, 1) > 0 THEN
    EXECUTE query2 INTO col_mode;
    END IF;
    
    end_ts := clock_timestamp();
    

    RAISE DEBUG 'AVERAGES: %', col_averages;
    RAISE INFO 'COMPUTE COLUMN AVERAGES: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

    -- CREATE TABLE WITH MISSING VALUES
    query := 'DROP TABLE IF EXISTS ' || output_table_name;
    RAISE DEBUG '%', query;
    EXECUTE QUERY;
    
    SELECT (
        SELECT array_agg(x || ' float8')
        FROM unnest(continuous_columns) AS x
    ) ||
    (
        SELECT array_agg(x || ' int')
        FROM unnest(categorical_columns) AS x
    ) ||
    (
        SELECT array_agg(x || '_ISNULL bool')
        FROM unnest(continuous_columns_null) AS x
    )||
    (
        SELECT array_agg(x || '_ISNULL bool')
        FROM unnest(categorical_columns_null) AS x
    )
    INTO tmp_array;
    
    query := 'CREATE UNLOGGED TABLE ' || output_table_name || '( ' ||
                array_to_string(tmp_array, ', ') || ', ROW_ID serial) WITH (fillfactor=75) ';
    RAISE DEBUG '%', query;
    EXECUTE QUERY;
        
        -----
        
    start_ts := clock_timestamp();
    SELECT (
        SELECT array_agg(
            CASE 
                WHEN array_position(continuous_columns_null, x) IS NULL THEN 
                    x
                ELSE 
                    'COALESCE(' || x || ', ' || col_averages[array_position(continuous_columns_null, x)] || ')'
            END
        )
        FROM unnest(continuous_columns) AS x
    ) || (
        SELECT array_agg(
            CASE 
                WHEN array_position(categorical_columns_null, x) IS NULL THEN 
                    x
                ELSE 
                    'COALESCE(' || x || ', ' || col_mode[array_position(categorical_columns_null, x)] || ')'
            END
        )
        FROM unnest(categorical_columns) AS x
    ) || (
        SELECT array_agg(x || ' IS NULL')
        FROM unnest(continuous_columns_null) AS x
    ) || (
        SELECT array_agg(x || ' IS NULL')
        FROM unnest(categorical_columns_null) AS x
    ) INTO tmp_array;
    
    
    
    query := 'INSERT INTO ' || output_table_name || 
             ' SELECT ' || array_to_string(tmp_array, ', ') ||
             ' FROM ' || input_table_name;
    RAISE DEBUG '%', query;
    
    EXECUTE query;
    end_ts := clock_timestamp();
    RAISE INFO 'INSERT INTO TABLE WITH MISSING VALUES: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts)); 
    
    ---1hot encoder
    IF array_length(categorical_columns, 1) > 0 THEN
    	FOR _counter in 1..cardinality(categorical_columns_null) LOOP
    		categorical_columns := array_remove(categorical_columns,  categorical_columns_null[_counter]);
    	END LOOP;
    END IF;
    IF array_length(categorical_columns, 1) > 0 THEN
    	query := 'SELECT madlib.encode_categorical_variables('''||output_table_name||''', '''||output_table_name||'_1hot'', ''' ||
                   array_to_string(categorical_columns, ', ') || ''');';
        RAISE NOTICE '%', query;
        start_ts := clock_timestamp();
        EXECUTE query;
        end_ts := clock_timestamp();
        RAISE INFO 'TIME 1-hot: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
        EXECUTE 'DROP TABLE '||output_table_name;
        query := 'SELECT array_agg(quote_ident(column_name)) FROM information_schema.columns WHERE column_name NOT LIKE ''%_isnull'' AND table_name = '''||output_table_name||'_1hot''';
        RAISE NOTICE '%', query;
        EXECUTE query INTO columns;
        output_table_name := output_table_name || '_1hot';
        COMMIT;
    END IF;

    
      
    _start_ts  := clock_timestamp();
    
    FOR _counter_2 in 1..cardinality(categorical_columns_null)
         LOOP
        	RAISE NOTICE 'imputing... %', categorical_columns_null[_counter_2];
            _indep_vars := ARRAY['1'] || array_remove(columns,  categorical_columns_null[_counter_2]);

            EXECUTE ('CREATE UNLOGGED TABLE tmp_table AS (SELECT * FROM '|| output_table_name|| ' WHERE ' || categorical_columns_null[_counter_2] || '_ISNULL IS false)');
            RAISE NOTICE 'train... %', categorical_columns_null[_counter_2];
            query := ('SELECT madlib.logregr_train( ''tmp_table'', ''res_logregr'' ,''' || categorical_columns_null[_counter_2] || ''', ''ARRAY[' || array_to_string(_indep_vars, ', ') || ']'', NULL, 1, ''irls'', 0.01, TRUE)');
            RAISE NOTICE '%', query;
            EXECUTE query;
            RAISE NOTICE 'predict... %', categorical_columns_null[_counter_2];
            --- predict
            query := ('SELECT '||array_to_string(_indep_vars,', ') ||', madlib.logregr_predict( m.coef, ' || ' ARRAY[' || array_to_string(_indep_vars, ', ') || ']) as predict FROM '|| output_table_name || ', res_logregr m WHERE ' || categorical_columns_null[_counter_2] || '_ISNULL IS true');
            
            EXECUTE ('UPDATE '|| output_table_name ||' SET ' || categorical_columns_null[_counter_2] || ' = p.predict::int FROM (' || query || ') AS p WHERE p.ROW_ID = '||output_table_name||'.ROW_ID');
            --_query := ('SELECT '||array_to_string(_indep_vars,', ') ||', madlib.logregr_predict( m.coef, ' || ' ARRAY[' || array_to_string(_indep_vars, ', ') || ']) as predict FROM '|| output_table_name || ', res_logregr m WHERE ' || categorical_columns_null[_counter_2] || '_ISNULL IS true');
            RAISE NOTICE '%', ('UPDATE '|| output_table_name || ' SET '||categorical_columns_null[_counter_2] || ' = madlib.logregr_predict( res_logregr.coef, ' || ' ARRAY[' || array_to_string(_indep_vars, ', ') || ']) WHERE '||categorical_columns_null[_counter_2] || '_ISNULL');

            --EXECUTE 'UPDATE '|| output_table_name || ' SET '||categorical_columns_null[_counter_2] || ' = madlib.logregr_predict( res_logregr.coef, ' || ' ARRAY[' || array_to_string(_indep_vars, ', ') || ']) WHERE '||categorical_columns_null[_counter_2] || '_ISNULL';
                        
                    --RAISE NOTICE 'DROP TABLE res_linregr';
            EXECUTE ('DROP TABLE IF EXISTS res_logregr');
        	EXECUTE 'DROP TABLE IF EXISTS res_logregr_summary';
            EXECUTE 'DROP TABLE IF EXISTS tmp_table';
            COMMIT;
        END LOOP;
    
        -----
    FOR _counter in 1..mice_iter
    LOOP
        RAISE NOTICE 'mice iteration... %', _counter;
            
        FOR _counter_2 in 1..cardinality(continuous_columns_null)
        LOOP
            RAISE NOTICE 'imputing... %', continuous_columns_null[_counter_2];
            _indep_vars := ARRAY['1'] || array_remove(columns,  continuous_columns_null[_counter_2]);
            RAISE NOTICE '%', 'CREATE UNLOGGED TABLE tmp_table AS (SELECT * FROM '||output_table_name||' WHERE ' || continuous_columns_null[_counter_2] || '_ISNULL IS false)';
            
            EXECUTE ('CREATE UNLOGGED TABLE tmp_table AS (SELECT * FROM '||output_table_name||' WHERE ' || continuous_columns_null[_counter_2] || '_ISNULL IS false)');
            
            RAISE NOTICE ' ... %', 'SELECT madlib.linregr_train( ''tmp_table'', ''res_linregr'' ,''' ||
            continuous_columns_null[_counter_2] || ''', ''ARRAY[' || array_to_string(_indep_vars, ', ') || ']'')';
            
            EXECUTE ('SELECT madlib.linregr_train( ''tmp_table'', ''res_linregr'' ,''' ||
            continuous_columns_null[_counter_2] || ''', ''ARRAY[' || array_to_string(_indep_vars, ', ') || ']'')');

            RAISE NOTICE 'trained, predicting... %', continuous_columns_null[_counter_2];

                --- predict
            _query := ('SELECT '||array_to_string(_indep_vars,', ') ||', madlib.linregr_predict( m.coef, ' || ' ARRAY[' || array_to_string(_indep_vars, ', ') || ']) as predict FROM '||output_table_name||', res_linregr m WHERE ' || continuous_columns_null[_counter_2] || '_ISNULL IS true');
            RAISE NOTICE '%', 'UPDATE '||output_table_name||' SET ' || continuous_columns_null[_counter_2] || ' = p.predict FROM (' || _query || ') AS p WHERE p.ROW_ID = '||output_table_name||'.ROW_ID';            
            EXECUTE ('UPDATE '||output_table_name||' SET ' || continuous_columns_null[_counter_2] || ' = p.predict FROM (' || _query || ') AS p WHERE p.ROW_ID = '||output_table_name||'.ROW_ID');
            EXECUTE ('DROP TABLE res_linregr');
            EXECUTE 'DROP TABLE res_linregr_summary';
            EXECUTE 'DROP TABLE tmp_table';
            COMMIT;
        END LOOP;  
        
          
    END LOOP;
        _end_ts  := clock_timestamp();
        RAISE NOTICE 'Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
END$$;

CALL MICE_madlib('join_table', 'join_table_complete', ARRAY['CRS_DEP_HOUR', 'CRS_DEP_MIN', 'CRS_ARR_HOUR', 'CRS_ARR_MIN', 'DISTANCE', 'DEP_DELAY', 'TAXI_OUT', 'TAXI_IN', 'ARR_DELAY', 'ACTUAL_ELAPSED_TIME', 'AIR_TIME', 'DEP_TIME_HOUR', 'DEP_TIME_MIN', 'WHEELS_OFF_HOUR', 'WHEELS_OFF_MIN', 'WHEELS_ON_HOUR', 'WHEELS_ON_MIN', 'ARR_TIME_HOUR', 'ARR_TIME_MIN', 'MONTH_SIN', 'MONTH_COS', 'DAY_SIN', 'DAY_COS', 'WEEKDAY_SIN', 'WEEKDAY_COS'], ARRAY['OP_CARRIER', 'DIVERTED', 'EXTRA_DAY_DEP', 'EXTRA_DAY_ARR']::text[], ARRAY['WHEELS_ON_HOUR', 'WHEELS_OFF_HOUR', 'TAXI_OUT', 'TAXI_IN', 'ARR_DELAY', 'DEP_DELAY'], ARRAY['DIVERTED']::text[]);

CALL MICE_madlib('join_table', 'join_table_complete', ARRAY['population', 'white', 'asian', 'pacific', 'black', 'medianage', 'occupiedhouseunits', 'houseunits', 'families', 'households', 'husbwife', 'males', 'females', 'householdschildren', 'hispanic', 'rgn_cd', 'clim_zn_nbr', 'tot_area_sq_ft', 'sell_area_sq_ft', 'avghhi', 'supertargetdistance', 'supertargetdrivetime', 'targetdistance', 'targetdrivetime', 'walmartdistance', 'walmartdrivetime', 'walmartsupercenterdistance', 'walmartsupercenterdrivetime' , 'prize', 'feat_1', 'maxtemp', 'mintemp', 'meanwind', 'inventoryunits'], ARRAY['subcategory', 'category', 'categoryCluster', 'rain', 'snow', 'thunder']::text[], ARRAY['inventoryunits', 'maxtemp', 'mintemp', 'supertargetdistance', 'walmartdistance'], ARRAY['rain', 'snow']::text[]);

CALL MICE_madlib('join_table', 'join_table_complete', ARRAY['AQI', 'CO', 'O3', 'PM10', 'PM2_5', 'NO2', 'NOx', 'NO_', 'WindSpeed', 'WindDirec', 'SO2_AVG'], ARRAY[]::text[], ARRAY['CO', 'O3', 'PM10', 'PM2_5', 'NO2', 'NOx', 'NO_', 'WindSpeed', 'WindDirec', 'SO2_AVG'], ARRAY[]::text[]);
--SET client_min_messages TO DEBUG;
