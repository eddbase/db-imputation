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
        mice_iter int := 5;
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
    	query text;
    	query2 text;
    	start_ts timestamptz;
    	end_ts   timestamptz;
    	col_averages float8[];
    	col_mode int4[];
		indep_vars text[];



    BEGIN
    columns := continuous_columns || categorical_columns;
    ---drop table res_linregr_summary, res_linregr    
    SELECT array_agg('AVG(' || x || ')')
    FROM unnest(continuous_columns_null) AS x
    INTO tmp_array;
    
    SELECT array_agg('MODE() WITHIN GROUP (ORDER BY ' || x || ') ')
    FROM unnest(categorical_columns_null) AS x
    INTO tmp_array2;
    
    query := ' SELECT ARRAY[ ' || array_to_string(tmp_array, ', ') || ' ]::float8[]' || 
             ' FROM ( SELECT ' || array_to_string(continuous_columns_null, ', ') || 
                    ' FROM ' || input_table_name || ' LIMIT 100000 ) AS t';
    query2 := ' SELECT ARRAY[ ' || array_to_string(tmp_array2, ', ') || ' ]::int[]' || 
             ' FROM ( SELECT ' || array_to_string(categorical_columns_null, ', ') || 
                    ' FROM ' || input_table_name || ' LIMIT 100000 ) AS t';
    
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
                array_to_string(tmp_array, ', ') || ')';
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
    _start_ts  := clock_timestamp();
        -----
    FOR _counter in 1..mice_iter
    LOOP
        RAISE NOTICE 'mice iteration... %', _counter;
         FOR _counter_2 in 1..cardinality(categorical_columns_null)
         LOOP
        	RAISE NOTICE 'imputing... %', categorical_columns_null[_counter_2];
            _indep_vars := ARRAY['1'] || array_remove(columns,  categorical_columns_null[_counter_2]);

            EXECUTE ('CREATE UNLOGGED TABLE tmp_table AS (SELECT * FROM '|| output_table_name|| ' WHERE ' || categorical_columns_null[_counter_2] || '_ISNULL IS false)');
            RAISE NOTICE 'train... %', categorical_columns_null[_counter_2];
            EXECUTE ('SELECT madlib.logregr_train( ''tmp_table'', ''res_logregr'' ,''' || categorical_columns_null[_counter_2] || ''', ''ARRAY[' || array_to_string(_indep_vars, ', ') || ']'')');
            RAISE NOTICE 'predict... %', categorical_columns_null[_counter_2];
            --- predict
            _query := ('SELECT '||array_to_string(_indep_vars,', ') ||', madlib.logregr_predict( m.coef, ' || ' ARRAY[' || array_to_string(_indep_vars, ', ') || ']) as predict FROM '|| output_table_name || ', res_logregr m WHERE ' || categorical_columns_null[_counter_2] || '_ISNULL IS true');
            
            --EXECUTE ('UPDATE '|| output_table_name ||' SET ' || categorical_columns_null[_counter_2] || ' = p.predict::int FROM (' || _query || ') AS p WHERE p.id = '||output_table_name||'.id');
            
            EXECUTE 'UPDATE '|| output_table_name || ' SET '||categorical_columns_null[_counter_2] || ' = madlib.logregr_predict( m.coef, ' || ' ARRAY[' || array_to_string(_indep_vars, ', ') || ']) WHERE '||categorical_columns_null[_counter_2] || '_ISNULL';
                        
                    --RAISE NOTICE 'DROP TABLE res_linregr';
            EXECUTE ('DROP TABLE IF EXISTS res_logregr');
        	EXECUTE 'DROP TABLE IF EXISTS res_logregr_summary';
            EXECUTE 'DROP TABLE IF EXISTS tmp_table';
            COMMIT;
        END LOOP;
            
        FOR _counter_2 in 1..cardinality(num_columns_null)
        LOOP
            RAISE NOTICE 'imputing... %', num_columns_null[_counter_2];
            indep_vars := ARRAY['1'] || array_remove(columns,  num_columns_null[_counter_2]);

            EXECUTE ('CREATE TEMPORARY TABLE tmp_table AS (SELECT * FROM '||output_table_name||' WHERE ' || num_columns_null[_counter_2] || '_ISNULL IS false)');
            EXECUTE ('SELECT madlib.linregr_train( ''tmp_table'', ''res_linregr'' ,''' ||
            num_columns_null[_counter_2] || ''', ''ARRAY[' || array_to_string(_indep_vars, ', ') || ']'')');

            RAISE NOTICE 'trained, predicting... %', num_columns_null[_counter_2];

                --- predict
            _query := ('SELECT '||array_to_string(_indep_vars,', ') ||', madlib.linregr_predict( m.coef, ' || ' ARRAY[' || array_to_string(_indep_vars, ', ') || ']) as predict FROM '||output_table_name||', res_linregr m WHERE ' || num_columns_null[_counter_2] || '_ISNULL IS true');
                
            EXECUTE ('UPDATE '||output_table_name||' SET ' || num_columns_null[_counter_2] || ' = p.predict FROM (' || _query || ') AS p WHERE p.id = '||output_table_name||'.id');
            EXECUTE ('DROP TABLE res_linregr');
            EXECUTE 'DROP TABLE res_linregr_summary';
            EXECUTE 'DROP TABLE tmp_table';
            COMMIT;
        END LOOP;    
    END LOOP;
        _end_ts  := clock_timestamp();
        RAISE NOTICE 'Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
        RAISE NOTICE 'Execution time (JOIN) in ms = %' , 1000 * (extract(epoch FROM _int_ts - _start_ts));

END$$;

CALL MICE_madlib('join_table', 'join_table_complete', ARRAY['CRS_DEP_HOUR', 'CRS_DEP_MIN', 'CRS_ARR_HOUR', 'CRS_ARR_MIN', 'DISTANCE', 'DEP_DELAY', 'TAXI_OUT', 'TAXI_IN', 'ARR_DELAY', 'ACTUAL_ELAPSED_TIME', 'AIR_TIME', 'DEP_TIME_HOUR', 'DEP_TIME_MIN', 'WHEELS_OFF_HOUR', 'WHEELS_OFF_MIN', 'WHEELS_ON_HOUR', 'WHEELS_ON_MIN', 'ARR_TIME_HOUR', 'ARR_TIME_MIN', 'MONTH_SIN', 'MONTH_COS', 'DAY_SIN', 'DAY_COS', 'WEEKDAY_SIN', 'WEEKDAY_COS'], ARRAY['OP_CARRIER', 'DIVERTED', 'EXTRA_DAY_DEP', 'EXTRA_DAY_ARR']::text[], ARRAY['WHEELS_ON_HOUR', 'WHEELS_OFF_HOUR', 'TAXI_OUT', 'TAXI_IN', 'ARR_DELAY', 'DEP_DELAY'], ARRAY['DIVERTED']::text[]);

CREATE UNLOGGED TABLE join_table AS (SELECT * FROM (flight.schedule JOIN flight.distances USING(airports)) AS s JOIN flight.flights USING (flight))


