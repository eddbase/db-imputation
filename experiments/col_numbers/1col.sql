CREATE OR REPLACE PROCEDURE MICE_incremental_partitioned(
        input_table_name text,
        output_table_name text,
        continuous_columns text[],
        categorical_columns text[],
        continuous_columns_null text[], 
        categorical_columns_null text[]
    ) LANGUAGE plpgsql AS $$
DECLARE 
    start_ts timestamptz;
    end_ts   timestamptz;
    query text;
    query2 text;
    subquery text;
    query_null1 text;
    query_null2 text;
    col_averages float8[];
    col_mode int4[];
    tmp_array text[];
    tmp_array2 text[];
    tmp_array3 text[];
    cofactor_global cofactor;
    cofactor_null cofactor;
    col text;
    params float8[];
    tmp_columns_names text[];
    label_index int4;
    max_range int4;
    --categorical_uniq_vals_sorted int[] := ARRAY[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28, 29,30,31,32,3,14,15,31,32,33,35,40,64,65,48,102,104,105,106,107,108,109,0,1,0,1];--categorical columns sorted by values -> RETAILER
    --upper_bound_categorical  int[] := ARRAY[32,42,50,52,54];--5,9,11 
    
    categorical_uniq_vals_sorted int[] := ARRAY[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22, 0,1];--categorical columns sorted by values  -> FLIGHTS
    upper_bound_categorical  int[] := ARRAY[23,25];--5,9,11 

    low_bound_categorical  int[]; 

BEGIN
    -- COMPUTE COLUMN AVERAGES (over a subset)
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
     || (
        ARRAY [ 'NULL_CNT int', 'NULL_COL_ID int', 'ROW_ID serial' ]
    )
    INTO tmp_array;
    
    query := 'CREATE UNLOGGED TABLE ' || output_table_name || '( ' ||
                array_to_string(tmp_array, ', ') || ') PARTITION BY RANGE (NULL_CNT)';
    RAISE DEBUG '%', query;
    EXECUTE QUERY;

    query := 'CREATE UNLOGGED TABLE ' || output_table_name || '_nullcnt0' || 
             ' PARTITION OF ' || output_table_name || 
             ' FOR VALUES FROM (0) TO (1) WITH (fillfactor=100)';
    RAISE DEBUG '%', query;
    EXECUTE QUERY;

    query := 'CREATE UNLOGGED TABLE ' || output_table_name || '_nullcnt1' || 
             ' PARTITION OF ' || output_table_name || 
             ' FOR VALUES FROM (1) TO (2) PARTITION BY RANGE (NULL_COL_ID)';
    RAISE DEBUG '%', query;
    EXECUTE QUERY;
    
    if array_length(categorical_columns_null, 1) > 0 then
        max_range := array_length(continuous_columns_null, 1) + array_length(categorical_columns_null, 1);
    else
        max_range := array_length(continuous_columns_null, 1);
    end if;
    
    
    FOR col_id in 1..max_range LOOP
        query := 'CREATE UNLOGGED TABLE ' || output_table_name || '_nullcnt1_col' || col_id || 
                 ' PARTITION OF ' || output_table_name || '_nullcnt1' || 
                 ' FOR VALUES FROM (' || col_id || ') TO (' || col_id + 1 || ') WITH (fillfactor=80)';
        RAISE DEBUG '%', query;
        EXECUTE QUERY;
    END LOOP;
    
    IF max_range >= 2 THEN
    	    if array_length(categorical_columns_null, 1) > 0 then
    			query := 'CREATE UNLOGGED TABLE ' || output_table_name || '_nullcnt2' || 
             	' PARTITION OF ' || output_table_name ||
             	' FOR VALUES FROM (2) TO (' || array_length(continuous_columns_null, 1) + array_length(categorical_columns_null, 1) + 1 || ') WITH (fillfactor=70)';
             else
            	query := 'CREATE UNLOGGED TABLE ' || output_table_name || '_nullcnt2' || 
             	' PARTITION OF ' || output_table_name ||
             	' FOR VALUES FROM (2) TO (' || array_length(continuous_columns_null, 1) + 1 || ') WITH (fillfactor=70)';
             end if;
    RAISE DEBUG '%', query;
    EXECUTE QUERY;
    END IF;
    
    COMMIT;
    
    -- INSERT INTO TABLE WITH MISSING VALUES
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
    ) || ( 
        SELECT ARRAY [ array_to_string(array_agg('(CASE WHEN ' || x || ' IS NULL THEN 1 ELSE 0 END)'), ' + ') ]
        FROM unnest(ARRAY['WHEELS_ON_HOUR']) AS x
    ) INTO tmp_array;--edit here
    
    SELECT (
        SELECT array_agg(
        		CASE 
        			WHEN array_position(continuous_columns_null, x) IS NULL THEN
        				' WHEN ' || x || ' IS NULL THEN ' || array_position (categorical_columns_null, x) + array_length(continuous_columns_null, 1)
                    ELSE 
                    	' WHEN ' || x || ' IS NULL THEN ' || array_position (continuous_columns_null, x)
                END
        )         
        FROM unnest(ARRAY['WHEELS_ON_HOUR']) AS x--edit here
    )
    INTO tmp_array2;
    RAISE DEBUG 'A: %', tmp_array;
    RAISE DEBUG 'B: %', tmp_array2;

    
    
    query := 'INSERT INTO ' || output_table_name || 
             ' SELECT ' || array_to_string(tmp_array, ', ') || ', CASE ' || array_to_string(tmp_array2, '  ') || ' ELSE 0 END ' ||
             ' FROM ' || input_table_name;
    RAISE DEBUG '%', query;

    EXECUTE query;
    end_ts := clock_timestamp();
    RAISE INFO 'INSERT INTO TABLE WITH MISSING VALUES: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));   
    
    -- COMPUTE MAIN COFACTOR
    query := 'SELECT SUM(to_cofactor(' ||
                    'ARRAY[ ' || array_to_string(continuous_columns, ', ') || ' ]::float8[],' ||
                    'ARRAY[ ' || array_to_string(categorical_columns, ', ') || ' ]::int4[]' ||
                ')) '
            'FROM ' || output_table_name;
    RAISE DEBUG '%', query;

    start_ts := clock_timestamp();
    EXECUTE query INTO STRICT cofactor_global;
    end_ts := clock_timestamp();
    RAISE INFO 'COFACTOR global: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
    
    COMMIT;
    
    FOR i in 1..5 LOOP
        RAISE INFO 'Iteration %', i;
        
        --edit here
        categorical_columns_null := ARRAY[]::text[];
        continuous_columns_null := ARRAY['WHEELS_ON_HOUR'];
        
        
        FOREACH col in ARRAY categorical_columns_null LOOP
            RAISE INFO '  |- Column %', col;
            
            -- COMPUTE COFACTOR WHERE $col IS NULL
            query_null1 := 'SELECT SUM(to_cofactor(' ||
                    'ARRAY[ ' || array_to_string(continuous_columns, ', ') || ' ]::float8[],' ||
                    'ARRAY[ ' || array_to_string(categorical_columns, ', ') || ' ]::int4[]' ||
                ')) '
            'FROM ' || output_table_name || ' '
            'WHERE (NULL_CNT = 1 AND NULL_COL_ID = ' || array_position(categorical_columns_null, col) + array_length(continuous_columns_null, 1) || ');';
            RAISE DEBUG '%', query_null1;
            
            start_ts := clock_timestamp();
            EXECUTE query_null1 INTO STRICT cofactor_null;
            end_ts := clock_timestamp();
            RAISE INFO 'COFACTOR NULL (1): ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
            -- RAISE DEBUG 'COFACTOR NULL = %', cofactor_null;

            IF cofactor_null IS NOT NULL THEN
                cofactor_global := cofactor_global - cofactor_null;
            END IF;
            
            --RAISE DEBUG 'COFACTOR GLOBAL = %', cofactor_global;
            
            query_null2 := 'SELECT SUM(to_cofactor(' ||
                    'ARRAY[ ' || array_to_string(continuous_columns, ', ') || ' ]::float8[],' ||
                    'ARRAY[ ' || array_to_string(categorical_columns, ', ') || ' ]::int4[]' ||
                ')) '
            'FROM ' || output_table_name || ' '
            'WHERE NULL_CNT >= 2 AND ' || col || '_ISNULL;';
            RAISE DEBUG '%', query_null2;
            
            start_ts := clock_timestamp();
            EXECUTE query_null2 INTO STRICT cofactor_null;
            end_ts := clock_timestamp();
            RAISE INFO 'COFACTOR NULL (2): ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
            -- RAISE DEBUG 'COFACTOR NULL = %', cofactor_null;

            IF cofactor_null IS NOT NULL THEN
                cofactor_global := cofactor_global - cofactor_null;
            END IF;
            
            RAISE DEBUG 'COFACTOR GLOBAL = %', cofactor_global;

            -- TRAIN
            
            label_index := array_position(categorical_columns, col);
            RAISE DEBUG 'LABEL INDEX %', query_null1;
            start_ts := clock_timestamp();
            params := lda_train(cofactor_global, label_index - 1, 0);
            end_ts := clock_timestamp();
            --RAISE DEBUG '%', params;
            RAISE DEBUG ' cat columns 2: %',categorical_columns ;
            RAISE DEBUG ' cat columns null 2: %', categorical_columns_null;

            RAISE INFO 'TRAIN ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
            
            -- IMPUTE
            start_ts := clock_timestamp();
            
            --skip current label in categorical features
            --cat columns: letters
            
            low_bound_categorical := ARRAY[0] || array_remove(upper_bound_categorical , upper_bound_categorical[array_upper(upper_bound_categorical, 1)]);
            --offset wrong when label skipped
            RAISE DEBUG ' cat columns 3: %',categorical_columns ;
            RAISE DEBUG ' cat columns null 3: %', categorical_columns_null;

            IF array_length(categorical_columns,1) = 1 AND array_length(categorical_columns_null,1) = 1 THEN --only 1 cat. var. null, so no cat. features
            	subquery := 'ARRAY[]';
            ELSE
	            subquery := '(SELECT array_agg(array_position((ARRAY[' || array_to_string(categorical_uniq_vals_sorted, ' ,') || '])[bound_down+1 : bound_up], a.elem) - 1 + bound_down - CASE WHEN a.nr < ' || label_index || ' THEN 0 ELSE ' || upper_bound_categorical[label_index] ||' END)' ||
    	        ' FROM unnest(ARRAY[' || array_to_string(categorical_columns, ', ') || ']::int[], ARRAY[' || array_to_string(upper_bound_categorical , ', ') || ']::int[], ARRAY[' || array_to_string(low_bound_categorical , ', ') ||']::int[]) WITH ORDINALITY a(elem, bound_up, bound_down, nr) ' ||
       		 	' WHERE a.nr != ' || label_index ||')';
       		END IF;

            RAISE DEBUG ' SUBQUERY: %', subquery;
            
                        
            --query := 'UPDATE ' || output_table_name || 
            --    ' SET ' || col || ' = (ARRAY[' || array_to_string(categorical_uniq_vals_sorted[low_bound_categorical[label_index]+1 : upper_bound_categorical[label_index]], ', ') ||'])[1+ lda_impute(ARRAY[ ' || array_to_string(params, ', ') || ']::float4[], ' || 'ARRAY[ ' || array_to_string(continuous_columns, ', ') || ' ]::float4[], '
             --   || subquery || ' ::int[])] ' ||
             --   ' WHERE NULL_CNT = 1 AND NULL_COL_ID = ' || array_position(categorical_columns_null, col) + array_length(continuous_columns_null, 1)  || ';';
            -- RAISE DEBUG 'UPDATE QUERY: %', query;
            
            query := 'SELECT array_agg(quote_ident(column_name)) FROM information_schema.columns WHERE table_name = ''' || output_table_name || ''';';
            EXECUTE query INTO tmp_columns_names;
            RAISE DEBUG '%', tmp_columns_names;
            
            query := 'ALTER TABLE '||output_table_name || '_nullcnt1 DETACH PARTITION '|| output_table_name ||'_nullcnt1_col'||array_position(categorical_columns_null, col)+ array_length(continuous_columns_null, 1);
            RAISE DEBUG '%', query;
            EXECUTE query;

            query := 'ALTER TABLE '||output_table_name ||'_nullcnt1_col'||array_position(categorical_columns_null, col)+ array_length(continuous_columns_null, 1)  ||' RENAME TO tmp_table';

            RAISE DEBUG '%', query;
            EXECUTE query;
            
            
            SELECT array_agg(
            	CASE WHEN array_position(categorical_columns_null, x) = label_index THEN
            	    '(ARRAY[' || array_to_string(categorical_uniq_vals_sorted[low_bound_categorical[label_index]+1 : upper_bound_categorical[label_index]], ', ') ||'])[1+ lda_impute(ARRAY[ ' || array_to_string(params, ', ') || ']::float4[], ' || 'ARRAY[ ' || array_to_string(continuous_columns, ', ') || ' ]::float4[], '
                || subquery || ' ::int[])] ' || ' AS ' || x
            	ELSE
            	    x || ' AS ' || x
            	END
            	)
            FROM unnest(tmp_columns_names) AS x
            INTO tmp_array3;
            
            RAISE DEBUG '%', tmp_array3;
            --EXECUTE query;
                        
            
            query := 'CREATE UNLOGGED TABLE ' || output_table_name || '_nullcnt1_col'||array_position(categorical_columns_null, col)+ array_length(continuous_columns_null, 1)  || ' AS SELECT ';
            query := query || array_to_string(tmp_array3, ' , ') ||' FROM tmp_table';
                    RAISE DEBUG '%', query;
            EXECUTE query;
        	query := 'ALTER TABLE '|| output_table_name || '_nullcnt1_col'||array_position(categorical_columns_null, col) + array_length(continuous_columns_null, 1)  ||' ALTER COLUMN row_id SET NOT NULL';
        	        RAISE DEBUG '%', query;
                    EXECUTE query;
            query := 'ALTER TABLE '|| output_table_name || '_nullcnt1 ATTACH PARTITION '||output_table_name || '_nullcnt1_col'||array_position(categorical_columns_null, col)+ array_length(continuous_columns_null, 1)  ||' FOR VALUES FROM (' || array_position(categorical_columns_null, col)+ array_length(continuous_columns_null, 1)  || ') TO (' || array_position(categorical_columns_null, col)+ array_length(continuous_columns_null, 1)  + 1 || ')';
                    RAISE DEBUG '%', query;
                    EXECUTE query;
                    EXECUTE 'DROP TABLE tmp_table';
            ----
            

            end_ts := clock_timestamp();
            RAISE INFO 'IMPUTE DATA (1): ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
            
            query := 'UPDATE ' || output_table_name || 
                ' SET ' || col || ' = (ARRAY[' || array_to_string(categorical_uniq_vals_sorted[low_bound_categorical[label_index]+1 : upper_bound_categorical[label_index]], ', ') ||'])[1+ lda_impute(ARRAY[ ' || array_to_string(params, ', ') || ']::float4[], ' || 'ARRAY[ ' || array_to_string(continuous_columns, ', ') || ' ]::float4[], '
                || subquery || ' ::int[])] ' ||
                ' WHERE NULL_CNT >= 2 AND ' || col || '_ISNULL;';
            RAISE DEBUG '%', query;

            start_ts := clock_timestamp();
            EXECUTE query;
            end_ts := clock_timestamp();
            RAISE INFO 'IMPUTE DATA (2): ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
            
            start_ts := clock_timestamp();
            EXECUTE query_null1 INTO STRICT cofactor_null;
            end_ts := clock_timestamp();
            RAISE INFO 'COFACTOR NULL (1): ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

            IF cofactor_null IS NOT NULL THEN
                cofactor_global := cofactor_global + cofactor_null;
            END IF;

            start_ts := clock_timestamp();
            EXECUTE query_null2 INTO STRICT cofactor_null;
            end_ts := clock_timestamp();
            RAISE INFO 'COFACTOR NULL (2): ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
            
            IF cofactor_null IS NOT NULL THEN
                cofactor_global := cofactor_global + cofactor_null;
            END IF;
            
            COMMIT;
            
        END LOOP;
        

        FOREACH col in ARRAY continuous_columns_null LOOP
            RAISE INFO '  |- Column %', col;
            
            -- COMPUTE COFACTOR WHERE $col IS NULL
            query_null1 := 'SELECT SUM(to_cofactor(' ||
                    'ARRAY[ ' || array_to_string(continuous_columns, ', ') || ' ]::float8[],' ||
                    'ARRAY[ ' || array_to_string(categorical_columns, ', ') || ' ]::int4[]' ||
                ')) '
            'FROM ' || output_table_name || ' '
            'WHERE (NULL_CNT = 1 AND NULL_COL_ID = ' || array_position(continuous_columns_null, col) || ');';
            RAISE DEBUG '%', query_null1;
            
            start_ts := clock_timestamp();
            EXECUTE query_null1 INTO STRICT cofactor_null;
            end_ts := clock_timestamp();
            RAISE INFO 'COFACTOR NULL (1): ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

            IF cofactor_null IS NOT NULL THEN
                cofactor_global := cofactor_global - cofactor_null;
            END IF;

            query_null2 := 'SELECT SUM(to_cofactor(' ||
                    'ARRAY[ ' || array_to_string(continuous_columns, ', ') || ' ]::float8[],' ||
                    'ARRAY[ ' || array_to_string(categorical_columns, ', ') || ' ]::int4[]' ||
                ')) '
            'FROM ' || output_table_name || ' '
            'WHERE NULL_CNT >= 2 AND ' || col || '_ISNULL;';
            RAISE DEBUG '%', query_null2;
            
            start_ts := clock_timestamp();
            EXECUTE query_null2 INTO STRICT cofactor_null;
            end_ts := clock_timestamp();
            RAISE INFO 'COFACTOR NULL (2): ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

            IF cofactor_null IS NOT NULL THEN
                cofactor_global := cofactor_global - cofactor_null;
            END IF;

            -- TRAIN
            label_index := array_position(continuous_columns_null, col);
            start_ts := clock_timestamp();
            params := ridge_linear_regression(cofactor_global, label_index - 1, 0.001, 0, 10000);
            end_ts := clock_timestamp();
            RAISE INFO 'TRAIN: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
            RAISE DEBUG '%', params;

            -- IMPUTE
            
            start_ts := clock_timestamp();
            
            query := 'SELECT array_agg(quote_ident(column_name)) FROM information_schema.columns WHERE table_name = ''' || output_table_name || ''';';
            EXECUTE query INTO tmp_columns_names;
            RAISE DEBUG '%', tmp_columns_names;
            
            query := 'ALTER TABLE '||output_table_name || '_nullcnt1 DETACH PARTITION '|| output_table_name ||'_nullcnt1_col'||array_position(continuous_columns_null, col);
            RAISE DEBUG '%', query;
            EXECUTE query;

            query := 'ALTER TABLE '||output_table_name ||'_nullcnt1_col'||array_position(continuous_columns_null, col) ||' RENAME TO tmp_table';

            RAISE DEBUG '%', query;
            EXECUTE query;
                        
            
            -- IMPUTE
            SELECT array_agg(x || ' * ' || params[array_position(continuous_columns, x) + 1])
            FROM unnest(continuous_columns) AS x
            WHERE array_position(continuous_columns, x) != label_index
            INTO tmp_array;
            
            low_bound_categorical := ARRAY[0] || array_remove(upper_bound_categorical , upper_bound_categorical[array_upper(upper_bound_categorical, 1)]);
            
            SELECT array_agg(params[a.bound_down + array_position(categorical_columns[a.bound_down+1:a.bound_up], a.x) + array_length(continuous_columns, 1)])
            FROM unnest(categorical_columns, upper_bound_categorical, low_bound_categorical) WITH ORDINALITY a(x, bound_up, bound_down, nr) 
            INTO tmp_array2;
            
            SELECT array_agg(
            	CASE WHEN array_position(continuous_columns_null, x) = label_index THEN
            	    array_to_string(array_prepend(params[1]::text, tmp_array || tmp_array2), ' + ') || ' AS ' || x
            	ELSE
            	    x || ' AS ' || x
            	END
            	)
            FROM unnest(tmp_columns_names) AS x
            INTO tmp_array3;
            
            RAISE DEBUG '%', tmp_array3;
            --EXECUTE query;
                        
            
            query := 'CREATE UNLOGGED TABLE ' || output_table_name || '_nullcnt1_col'||array_position(continuous_columns_null, col) || ' AS SELECT ';
            query := query || array_to_string(tmp_array3, ' , ') ||' FROM tmp_table';
                    RAISE DEBUG '%', query;
            EXECUTE query;
        	query := 'ALTER TABLE '|| output_table_name || '_nullcnt1_col'||array_position(continuous_columns_null, col) ||' ALTER COLUMN row_id SET NOT NULL';
        	        RAISE DEBUG '%', query;
                    EXECUTE query;
                    query := 'ALTER TABLE '|| output_table_name || '_nullcnt1 ATTACH PARTITION '||output_table_name || '_nullcnt1_col'||array_position(continuous_columns_null, col) ||' FOR VALUES FROM (' || array_position(continuous_columns_null, col) || ') TO (' || array_position(continuous_columns_null, col) + 1 || ')';
                    RAISE DEBUG '%', query;
                    EXECUTE query;
                    EXECUTE 'DROP TABLE tmp_table';
            
                        
            end_ts := clock_timestamp();
            RAISE INFO 'IMPUTE DATA (1): ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
            
            query := 'UPDATE ' || output_table_name || 
                ' SET ' || col || ' = ' || array_to_string(array_prepend(params[1]::text, tmp_array || tmp_array2), ' + ') ||
                ' WHERE NULL_CNT >= 2 AND ' || col || '_ISNULL;';
            RAISE DEBUG '%', query;

            start_ts := clock_timestamp();
            EXECUTE query;
            end_ts := clock_timestamp();
            RAISE INFO 'IMPUTE DATA (2): ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
            
            start_ts := clock_timestamp();
            EXECUTE query_null1 INTO STRICT cofactor_null;
            end_ts := clock_timestamp();
            RAISE INFO 'COFACTOR NULL (1): ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

            IF cofactor_null IS NOT NULL THEN
                cofactor_global := cofactor_global + cofactor_null;
            END IF;

            start_ts := clock_timestamp();
            EXECUTE query_null2 INTO STRICT cofactor_null;
            end_ts := clock_timestamp();
            RAISE INFO 'COFACTOR NULL (2): ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
            
            IF cofactor_null IS NOT NULL THEN
                cofactor_global := cofactor_global + cofactor_null;
            END IF;
            
            COMMIT;
            
        END LOOP;
        
    END LOOP;
END$$;

CREATE UNLOGGED TABLE join_table AS (SELECT * FROM retailer.Weather JOIN retailer.Location USING (locn) JOIN retailer.Inventory USING (locn, dateid) JOIN retailer.Item USING (ksn) JOIN retailer.Census USING (zip));

CREATE UNLOGGED TABLE join_table AS (SELECT * FROM flight.Route JOIN flight.schedule USING (ROUTE_ID) JOIN flight.flight USING (SCHEDULE_ID));



CALL MICE_incremental_partitioned('join_table', 'join_table_complete', ARRAY['population', 'white', 'asian', 'pacific', 'black', 'medianage', 'occupiedhouseunits', 'houseunits', 'families', 'households', 'husbwife', 'males', 'females', 'householdschildren', 'hispanic', 'rgn_cd', 'clim_zn_nbr', 'tot_area_sq_ft', 'sell_area_sq_ft', 'avghhi', 'supertargetdistance', 'supertargetdrivetime', 'targetdistance', 'targetdrivetime', 'walmartdistance', 'walmartdrivetime', 'walmartsupercenterdistance', 'walmartsupercenterdrivetime' , 'prize', 'feat_1', 'maxtemp', 'mintemp', 'meanwind', 'thunder', 'inventoryunits'], ARRAY['subcategory', 'category', 'categoryCluster', 'rain', 'snow']::text[], ARRAY['inventoryunits', 'maxtemp', 'mintemp', 'supertargetdistance', 'walmartdistance'], ARRAY['rain', 'snow']::text[]);


INSERT INTO join_table_complete SELECT population, white, asian, pacific, black, medianage, occupiedhouseunits, houseunits, families, households, husbwife, males, females, householdschildren, hispanic, rgn_cd, clim_zn_nbr, tot_area_sq_ft, sell_area_sq_ft, avghhi, COALESCE(supertargetdistance, 0.7729914682124249), supertargetdrivetime, targetdistance, targetdrivetime, COALESCE(walmartdistance, 1.0430703726497983), walmartdrivetime, walmartsupercenterdistance, walmartsupercenterdrivetime, prize, feat_1, COALESCE(maxtemp, 0.33746339820310167), COALESCE(mintemp, 0.20190808980641883), meanwind, thunder, COALESCE(inventoryunits, 0.013296220261212214), subcategory, category, categoryCluster, COALESCE(rain, 0), COALESCE(snow, 0), inventoryunits IS NULL, maxtemp IS NULL, mintemp IS NULL, supertargetdistance IS NULL, walmartdistance IS NULL, rain IS NULL, snow IS NULL, (CASE WHEN inventoryunits IS NULL THEN 1 ELSE 0 END) + (CASE WHEN maxtemp IS NULL THEN 1 ELSE 0 END) + (CASE WHEN mintemp IS NULL THEN 1 ELSE 0 END) + (CASE WHEN supertargetdistance IS NULL THEN 1 ELSE 0 END) + (CASE WHEN walmartdistance IS NULL THEN 1 ELSE 0 END) + (CASE WHEN rain IS NULL THEN 1 ELSE 0 END) + (CASE WHEN snow IS NULL THEN 1 ELSE 0 END), CASE  WHEN inventoryunits IS NULL THEN 1   WHEN maxtemp IS NULL THEN 2   WHEN mintemp IS NULL THEN 3   WHEN supertargetdistance IS NULL THEN 4   WHEN walmartdistance IS NULL THEN 5   WHEN rain IS NULL THEN 6   WHEN snow IS NULL THEN 7 ELSE 0 END, 0 FROM join_table
180s

create unlogged table tmp_table_2 as SELECT population, white, asian, pacific, black, medianage, occupiedhouseunits, houseunits, families, households, husbwife, males, females, householdschildren, hispanic, rgn_cd, clim_zn_nbr, tot_area_sq_ft, sell_area_sq_ft, avghhi, COALESCE(supertargetdistance, 0.7729914682124249) AS supertargetdistance, supertargetdrivetime, targetdistance, targetdrivetime, COALESCE(walmartdistance, 1.0430703726497983) AS walmartdistance, walmartdrivetime, walmartsupercenterdistance, walmartsupercenterdrivetime, prize, feat_1, COALESCE(maxtemp, 0.33746339820310167) AS maxtemp, COALESCE(mintemp, 0.20190808980641883) AS mintemp, meanwind, thunder, COALESCE(inventoryunits, 0.013296220261212214) AS inventoryunits, subcategory, category, categoryCluster, COALESCE(rain, 0) AS rain, COALESCE(snow, 0) as snow, inventoryunits IS NULL as inventoryunits_ISNULL, maxtemp IS NULL AS maxtemp_ISNULL, mintemp IS NULL AS mintemp_ISNULL, supertargetdistance IS NULL AS supertargetdistance_ISNULL, walmartdistance IS NULL as walmartdistance_ISNULL, rain IS NULL as rain_ISNULL, snow IS NULL as snow_ISNULL, (CASE WHEN inventoryunits IS NULL THEN 1 ELSE 0 END) + (CASE WHEN maxtemp IS NULL THEN 1 ELSE 0 END) + (CASE WHEN mintemp IS NULL THEN 1 ELSE 0 END) + (CASE WHEN supertargetdistance IS NULL THEN 1 ELSE 0 END) + (CASE WHEN walmartdistance IS NULL THEN 1 ELSE 0 END) + (CASE WHEN rain IS NULL THEN 1 ELSE 0 END) + (CASE WHEN snow IS NULL THEN 1 ELSE 0 END), CASE  WHEN inventoryunits IS NULL THEN 1   WHEN maxtemp IS NULL THEN 2   WHEN mintemp IS NULL THEN 3   WHEN supertargetdistance IS NULL THEN 4   WHEN walmartdistance IS NULL THEN 5   WHEN rain IS NULL THEN 6   WHEN snow IS NULL THEN 7 ELSE 0 END AS col_partition, 0 as id FROM join_table


----
--CALL MICE_incremental_partitioned('join_table', 'join_table_complete', ARRAY['id','DEP_DELAY', 'TAXI_OUT', 'TAXI_IN', 'ARR_DELAY', 'ACTUAL_ELAPSED_TIME', 'AIR_TIME', 'DEP_TIME_HOUR', 'DEP_TIME_MIN', 'WHEELS_OFF_HOUR', 'WHEELS_OFF_MIN', 'WHEELS_ON_HOUR', 'WHEELS_ON_MIN', 'ARR_TIME_HOUR', 'ARR_TIME_MIN', 'MONTH_SIN', 'MONTH_COS', 'DAY_SIN', 'DAY_COS', 'WEEKDAY_SIN', 'WEEKDAY_COS', 'EXTRA_DAY_DEP', 'EXTRA_DAY_ARR', 'SCHEDULE_ID', 'CRS_DEP_HOUR', 'CRS_DEP_MIN', 'CRS_ARR_HOUR', 'CRS_ARR_MIN', 'DISTANCE'], ARRAY['OP_CARRIER', 'DIVERTED']::text[], ARRAY['WHEELS_ON_HOUR', 'WHEELS_OFF_HOUR', 'TAXI_OUT', 'TAXI_IN', 'ARR_DELAY', 'DEP_DELAY'], ARRAY['DIVERTED']::text[]);


CALL MICE_incremental_partitioned('join_table', 'join_table_complete', ARRAY['CRS_DEP_HOUR', 'CRS_DEP_MIN', 'CRS_ARR_HOUR', 'CRS_ARR_MIN', 'DISTANCE', 'DEP_DELAY', 'TAXI_OUT', 'TAXI_IN', 'ARR_DELAY', 'ACTUAL_ELAPSED_TIME', 'AIR_TIME', 'DEP_TIME_HOUR', 'DEP_TIME_MIN', 'WHEELS_OFF_HOUR', 'WHEELS_OFF_MIN', 'WHEELS_ON_HOUR', 'WHEELS_ON_MIN', 'ARR_TIME_HOUR', 'ARR_TIME_MIN', 'MONTH_SIN', 'MONTH_COS', 'DAY_SIN', 'DAY_COS', 'WEEKDAY_SIN', 'WEEKDAY_COS'], ARRAY['OP_CARRIER', 'DIVERTED', 'EXTRA_DAY_DEP', 'EXTRA_DAY_ARR']::text[], ARRAY['WHEELS_ON_HOUR', 'WHEELS_OFF_HOUR', 'TAXI_OUT', 'TAXI_IN', 'ARR_DELAY', 'DEP_DELAY'], ARRAY['DIVERTED']::text[]);

-- SET client_min_messages TO INFO;

-- CALL MICE_incremental_partitioned('R', 'R_complete', ARRAY['A', 'B', 'C', 'D', 'E'], ARRAY[]::text[], ARRAY[ 'A', 'B', 'C'], ARRAY[]::text[]);
---WHEELS_ON_HOUR':0.005, 'WHEELS_OFF_HOUR':0.005, 'TAXI_OUT':0.005, 'TAXI_IN
---CREATE TEMPORARY TABLE join_table AS (SELECT * FROM flight.Route JOIN flight.schedule USING (ROUTE_ID) JOIN flight.flight USING (SCHEDULE_ID))

-- CALL MICE_incremental_partitioned('join_table', 'join_table_complete', ARRAY['dep_delay', 'taxi_out', 'taxi_in', 'arr_delay', 'diverted', 'actual_elapsed_time', 'air_time', 'dep_time_hour', 'dep_time_min', 'wheels_off_hour', 'wheels_off_min', 'wheels_on_hour', 'wheels_on_min', 'arr_time_hour', 'arr_time_min', 'month_sin', 'month_cos', 'day_sin', 'day_cos', 'weekday_sin', 'weekday_cos', 'extra_day_arr', 'extra_day_dep', 'distance', 'crs_dep_hour', 'crs_dep_min', 'crs_arr_hour', 'crs_arr_min' ], ARRAY[]::text[], ARRAY[ 'dep_delay', 'arr_delay', 'taxi_in', 'taxi_out', 'diverted', 'WHEELS_OFF_HOUR', 'WHEELS_ON_HOUR' ], ARRAY[]::text[]);