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
    query_null1 text;
    query_null2 text;
    col_averages float8[];
    tmp_array text[];
    tmp_array2 text[];
    cofactor_global cofactor;
    cofactor_null cofactor;
    col text;
    params float8[];
    label_index int4;
BEGIN
    -- COMPUTE COLUMN AVERAGES (over a subset)
    SELECT array_agg('AVG(' || x || ')')
    FROM unnest(continuous_columns_null) AS x
    INTO tmp_array;

    query := ' SELECT ARRAY[ ' || array_to_string(tmp_array, ', ') || ' ]::float8[]' || 
             ' FROM ( SELECT ' || array_to_string(continuous_columns_null, ', ') || 
                    ' FROM ' || input_table_name || ' LIMIT 100000 ) AS t';
    RAISE DEBUG '%', query;

    start_ts := clock_timestamp();
    EXECUTE query INTO col_averages;
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
    ) || (
        SELECT array_agg(x || '_ISNULL bool')
        FROM unnest(continuous_columns_null) AS x
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

    FOR col_id in 1..array_length(continuous_columns_null, 1) LOOP
        query := 'CREATE TEMPORARY TABLE ' || output_table_name || '_nullcnt1_col' || col_id || 
                 ' PARTITION OF ' || output_table_name || '_nullcnt1' || 
                 ' FOR VALUES FROM (' || col_id || ') TO (' || col_id + 1 || ') WITH (fillfactor=70)';
        RAISE DEBUG '%', query;
        EXECUTE QUERY;
    END LOOP;

    query := 'CREATE TEMPORARY TABLE ' || output_table_name || '_nullcnt2' || 
             ' PARTITION OF ' || output_table_name ||
             ' FOR VALUES FROM (2) TO (' || array_length(continuous_columns_null, 1) + 1 || ') WITH (fillfactor=70)';
    RAISE DEBUG '%', query;
    EXECUTE QUERY;
    
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
        SELECT array_agg(x || ' IS NULL')
        FROM unnest(continuous_columns_null) AS x
    ) || ( 
        SELECT ARRAY [ array_to_string(array_agg('(CASE WHEN ' || x || ' IS NULL THEN 1 ELSE 0 END)'), ' + ') ]
        FROM unnest(continuous_columns_null) AS x
    ) || (
        SELECT ARRAY [ 'CASE ' || 
                        array_to_string(array_agg('WHEN ' || x || ' IS NULL THEN ' || array_position(continuous_columns_null, x)), ' ') || 
                       ' ELSE 0 END' ]
        FROM unnest(continuous_columns_null) AS x
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
    
    COMMIT;
    
    FOR i in 1..5 LOOP
        RAISE INFO 'Iteration %', i;

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
            params := ridge_linear_regression(cofactor_global, label_index - 1, 0.001, 0, 10000);
            RAISE DEBUG '%', params;

            -- IMPUTE
            SELECT array_agg(x || ' * ' || params[array_position(continuous_columns, x) + 1])
            FROM unnest(continuous_columns) AS x
            WHERE array_position(continuous_columns, x) != label_index
            INTO tmp_array;

            query := 'UPDATE ' || output_table_name || 
                ' SET ' || col || ' = ' || array_to_string(array_prepend(params[1]::text, tmp_array), ' + ') ||
                ' WHERE NULL_CNT = 1 AND NULL_COL_ID = ' || array_position(continuous_columns_null, col) || ';';
            RAISE DEBUG '%', query;

            start_ts := clock_timestamp();
            EXECUTE query;
            end_ts := clock_timestamp();
            RAISE INFO 'IMPUTE DATA (1): ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
            
            query := 'UPDATE ' || output_table_name || 
                ' SET ' || col || ' = ' || array_to_string(array_prepend(params[1]::text, tmp_array), ' + ') ||
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

-- SET client_min_messages TO INFO;

-- CALL MICE_incremental_partitioned('R', 'R_complete', ARRAY['A', 'B', 'C', 'D', 'E'], ARRAY[]::text[], ARRAY[ 'A', 'B', 'C'], ARRAY[]::text[]);
---WHEELS_ON_HOUR':0.005, 'WHEELS_OFF_HOUR':0.005, 'TAXI_OUT':0.005, 'TAXI_IN
---CREATE TEMPORARY TABLE join_table AS (SELECT * FROM flight.Route JOIN flight.schedule USING (ROUTE_ID) JOIN flight.flight USING (SCHEDULE_ID))

-- CALL MICE_incremental_partitioned('join_table', 'join_table_complete', ARRAY['dep_delay', 'taxi_out', 'taxi_in', 'arr_delay', 'diverted', 'actual_elapsed_time', 'air_time', 'dep_time_hour', 'dep_time_min', 'wheels_off_hour', 'wheels_off_min', 'wheels_on_hour', 'wheels_on_min', 'arr_time_hour', 'arr_time_min', 'month_sin', 'month_cos', 'day_sin', 'day_cos', 'weekday_sin', 'weekday_cos', 'extra_day_arr', 'extra_day_dep', 'distance', 'crs_dep_hour', 'crs_dep_min', 'crs_arr_hour', 'crs_arr_min' ], ARRAY[]::text[], ARRAY[ 'dep_delay', 'arr_delay', 'taxi_in', 'taxi_out', 'diverted', 'WHEELS_OFF_HOUR', 'WHEELS_ON_HOUR' ], ARRAY[]::text[]);