CREATE OR REPLACE PROCEDURE MICE_incremental_partitioned_classifier(
        input_table_name text,
        output_table_name text,
        continuous_columns text[],
        categorical_columns text[],
        categorical_columns_null text[]
    ) LANGUAGE plpgsql AS $$
DECLARE 
    start_ts timestamptz;
    end_ts   timestamptz;
    query text;
    subquery text;
    query_null1 text;
    query_null2 text;
    col_averages float8[];
    tmp_array text[];
    tmp_array2 text[];
    cofactor_global cofactor;
    cofactor_null cofactor;
    col text;
    params float4[];
    label_index int4;
    categorical_uniq_vals_sorted int[] := ARRAY[0,1];--0,1,3,5,9,1,2,3,4,1,2
    upper_bound_categorical  int[] := ARRAY[2];--5,9,11 
    low_bound_categorical  int[]; 

BEGIN
    -- COMPUTE COLUMN AVERAGES (over a subset)
    SELECT array_agg('MODE() WITHIN GROUP (ORDER BY ' || x || ') ')
    FROM unnest(categorical_columns_null) AS x
    INTO tmp_array;
    
    RAISE DEBUG 'QUERY 1: %', tmp_array;
    

    query := ' SELECT ARRAY[ ' || array_to_string(tmp_array, ', ') || ' ]::int[]' || 
             ' FROM ( SELECT ' || array_to_string(categorical_columns_null, ', ') || 
                    ' FROM ' || input_table_name || ' LIMIT 100000 ) AS t';
    RAISE DEBUG 'QUERY 1: % QUERY 2: %', tmp_array, query;

    start_ts := clock_timestamp();
    EXECUTE query INTO col_averages;
    end_ts := clock_timestamp();

    RAISE DEBUG 'MODES: %', col_averages;
    RAISE INFO 'COMPUTE COLUMN MODES: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

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
        FROM unnest(categorical_columns_null) AS x
    ) || (
        ARRAY [ 'NULL_CNT int NOT NULL', 'NULL_COL_ID int NOT NULL', 'ROW_ID serial' ]
    )
    INTO tmp_array;

    query := 'CREATE TEMPORARY TABLE ' || output_table_name || '( ' ||
                array_to_string(tmp_array, ', ') || ') PARTITION BY RANGE (NULL_CNT)';
    RAISE DEBUG '%', query;
    EXECUTE QUERY;

    query := 'CREATE TEMPORARY TABLE ' || output_table_name || '_nullcnt0' || 
             ' PARTITION OF ' || output_table_name || 
             ' FOR VALUES FROM (0) TO (1) WITH (fillfactor=100)';
    RAISE DEBUG '%', query;
    EXECUTE QUERY;

    query := 'CREATE TEMPORARY TABLE ' || output_table_name || '_nullcnt1' || 
             ' PARTITION OF ' || output_table_name || 
             ' FOR VALUES FROM (1) TO (2) PARTITION BY RANGE (NULL_COL_ID)';
    RAISE DEBUG '%', query;
    EXECUTE QUERY;

    FOR col_id in 1..array_length(categorical_columns_null, 1) LOOP
        query := 'CREATE TEMPORARY TABLE ' || output_table_name || '_nullcnt1_col' || col_id || 
                 ' PARTITION OF ' || output_table_name || '_nullcnt1' || 
                 ' FOR VALUES FROM (' || col_id || ') TO (' || col_id + 1 || ') WITH (fillfactor=70)';
        RAISE DEBUG '%', query;
        EXECUTE QUERY;
    END LOOP;
    
    IF array_length(categorical_columns_null, 1) > 1 THEN

    	query := 'CREATE TEMPORARY TABLE ' || output_table_name || '_nullcnt2' || 
            	 ' PARTITION OF ' || output_table_name ||
         	    ' FOR VALUES FROM (2) TO (' || array_length(categorical_columns_null, 1) + 1 || ') WITH (fillfactor=70)';
    	RAISE DEBUG '%', query;
    	EXECUTE QUERY;
    END IF;
    
    
    COMMIT;
    
    -- INSERT INTO TABLE WITH MISSING VALUES
    start_ts := clock_timestamp();
    SELECT (
    	SELECT array_agg(x)
        FROM unnest(continuous_columns) AS x
    ) || (
        SELECT array_agg(
            CASE 
                WHEN array_position(categorical_columns_null, x) IS NULL THEN 
                    x
                ELSE 
                    'COALESCE(' || x || ', ' || col_averages[array_position(categorical_columns_null, x)] || ')'
            END
        )
        FROM unnest(categorical_columns) AS x
    ) || (
        SELECT array_agg(x || ' IS NULL')
        FROM unnest(categorical_columns_null) AS x
    ) || ( 
        SELECT ARRAY [ array_to_string(array_agg('(CASE WHEN ' || x || ' IS NULL THEN 1 ELSE 0 END)'), ' + ') ]
        FROM unnest(categorical_columns_null) AS x
    ) || (
        SELECT ARRAY [ 'CASE ' || 
                        array_to_string(array_agg('WHEN ' || x || ' IS NULL THEN ' || array_position(categorical_columns_null, x)), ' ') || 
                       ' ELSE 0 END' ]
        FROM unnest(categorical_columns_null) AS x
    )
    INTO tmp_array;
    
    
    query := 'INSERT INTO ' || output_table_name || 
             ' SELECT ' || array_to_string(tmp_array, ', ') || ', 0' ||
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
    RAISE DEBUG 'COFACTOR = %', cofactor_global;
    
    COMMIT;
    
    FOR i in 1..5 LOOP
        RAISE INFO 'Iteration %', i;

        FOREACH col in ARRAY categorical_columns_null LOOP
            RAISE INFO '  |- Column %', col;
            
            -- COMPUTE COFACTOR WHERE $col IS NULL
            query_null1 := 'SELECT SUM(to_cofactor(' ||
                    'ARRAY[ ' || array_to_string(continuous_columns, ', ') || ' ]::float8[],' ||
                    'ARRAY[ ' || array_to_string(categorical_columns, ', ') || ' ]::int4[]' ||
                ')) '
            'FROM ' || output_table_name || ' '
            'WHERE (NULL_CNT = 1 AND NULL_COL_ID = ' || array_position(categorical_columns_null, col) || ');';
            RAISE DEBUG '%', query_null1;
            
            start_ts := clock_timestamp();
            EXECUTE query_null1 INTO STRICT cofactor_null;
            end_ts := clock_timestamp();
            RAISE INFO 'COFACTOR NULL (1): ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
            RAISE DEBUG 'COFACTOR NULL = %', cofactor_null;

            IF cofactor_null IS NOT NULL THEN
                cofactor_global := cofactor_global - cofactor_null;
            END IF;
            
            RAISE DEBUG 'COFACTOR GLOBAL = %', cofactor_global;
            
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
            RAISE DEBUG 'COFACTOR NULL = %', cofactor_null;

            IF cofactor_null IS NOT NULL THEN
                cofactor_global := cofactor_global - cofactor_null;
            END IF;
            
            RAISE DEBUG 'COFACTOR GLOBAL = %', cofactor_global;

            -- TRAIN
            
            label_index := array_position(categorical_columns, col);
            RAISE DEBUG 'LABEL INDEX %', query_null1;
            start_ts := clock_timestamp();
            params := lda_train(cofactor_global, label_index - 1, 0.4);
            end_ts := clock_timestamp();
            RAISE DEBUG '%', params;
            RAISE INFO 'TRAIN ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
            
            -- IMPUTE
            
            --skip current label in categorical features
            --cat columns: letters
            
            low_bound_categorical := ARRAY[0] || array_remove(upper_bound_categorical , upper_bound_categorical[array_upper(upper_bound_categorical, 1)]);
            --offset wrong when label skipped
            IF array_lower(categorical_columns,1) = 1 AND array_lower(categorical_columns_null,1) = 1 THEN --only 1 cat. var. null, so no cat. features
            	subquery := 'ARRAY[]';
            ELSE
	            subquery := '(SELECT array_agg(array_position((ARRAY[' || array_to_string(categorical_uniq_vals_sorted, ' ,') || '])[bound_down+1 : bound_up], a.elem) - 1 + bound_down - CASE WHEN a.nr < ' || label_index || ' THEN 0 ELSE ' || upper_bound_categorical[label_index] ||' END)' ||
    	        ' FROM unnest(ARRAY[' || array_to_string(categorical_columns, ', ') || ']::int[], ARRAY[' || array_to_string(upper_bound_categorical , ', ') || ']::int[], ARRAY[' || array_to_string(low_bound_categorical , ', ') ||']::int[]) WITH ORDINALITY a(elem, bound_up, bound_down, nr) ' ||
       		 	' WHERE a.nr != ' || label_index ||')';
       		END IF;

            RAISE DEBUG ' %', subquery;
            
                        
            query := 'UPDATE ' || output_table_name || 
                ' SET ' || col || ' = (ARRAY[' || array_to_string(categorical_uniq_vals_sorted[low_bound_categorical[label_index]+1 : upper_bound_categorical[label_index]], ', ') ||'])[1+ lda_impute(ARRAY[ ' || array_to_string(params, ', ') || ']::float4[], ' || 'ARRAY[ ' || array_to_string(continuous_columns, ', ') || ' ]::float4[], '
                || subquery || ' ::int[])] ' ||
                ' WHERE NULL_CNT = 1 AND NULL_COL_ID = ' || array_position(categorical_columns_null, col) || ';';
            RAISE DEBUG 'UPDATE QUERY: %', query;

            start_ts := clock_timestamp();
            EXECUTE query;
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
    END LOOP;
    
END$$;

--CALL MICE_incremental_partitioned_classifier('test2', 'test2_complete', ARRAY['a'], ARRAY['b', 'c', 'd']::text[], ARRAY['b', 'd']);

CREATE UNLOGGED TABLE join_table AS (SELECT * FROM flight.Route JOIN flight.schedule USING (ROUTE_ID) JOIN flight.flight USING (SCHEDULE_ID));
CALL MICE_incremental_partitioned_classifier('join_table', 'join_table_complete', ARRAY['dep_delay', 'taxi_out', 'taxi_in', 'arr_delay', 'actual_elapsed_time', 'air_time', 'dep_time_hour', 'dep_time_min', 'wheels_off_hour', 'wheels_off_min', 'wheels_on_hour', 'wheels_on_min', 'arr_time_hour', 'arr_time_min', 'month_sin', 'month_cos', 'day_sin', 'day_cos', 'weekday_sin', 'weekday_cos', 'extra_day_arr', 'extra_day_dep', 'distance', 'crs_dep_hour', 'crs_dep_min', 'crs_arr_hour', 'crs_arr_min' ], ARRAY['diverted']::text[], ARRAY['diverted']);


-- SET client_min_messages TO DEBUG;

-- CALL MICE_incremental_partitioned('R', 'R_complete', ARRAY['A', 'B', 'C', 'D', 'E'], ARRAY[]::text[], ARRAY[ 'A', 'B', 'C'], ARRAY[]::text[]);
---WHEELS_ON_HOUR':0.005, 'WHEELS_OFF_HOUR':0.005, 'TAXI_OUT':0.005, 'TAXI_IN
---CREATE TEMPORARY TABLE join_table AS (SELECT * FROM flight.Route JOIN flight.schedule USING (ROUTE_ID) JOIN flight.flight USING (SCHEDULE_ID))

-- CALL MICE_incremental_partitioned('join_table', 'join_table_complete', ARRAY['dep_delay', 'taxi_out', 'taxi_in', 'arr_delay', 'diverted', 'actual_elapsed_time', 'air_time', 'dep_time_hour', 'dep_time_min', 'wheels_off_hour', 'wheels_off_min', 'wheels_on_hour', 'wheels_on_min', 'arr_time_hour', 'arr_time_min', 'month_sin', 'month_cos', 'day_sin', 'day_cos', 'weekday_sin', 'weekday_cos', 'extra_day_arr', 'extra_day_dep', 'distance', 'crs_dep_hour', 'crs_dep_min', 'crs_arr_hour', 'crs_arr_min' ], ARRAY[]::text[], ARRAY[ 'dep_delay', 'arr_delay', 'taxi_in', 'taxi_out', 'diverted', 'WHEELS_OFF_HOUR', 'WHEELS_ON_HOUR' ], ARRAY[]::text[]);