CREATE OR REPLACE PROCEDURE MICE_baseline(
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
    query_null text;
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
    )
    INTO tmp_array;

    query := 'CREATE TEMPORARY TABLE ' || output_table_name || '( ' ||
                array_to_string(tmp_array, ', ') || ', ROW_ID serial) WITH (fillfactor=70);';
    RAISE DEBUG '%', query;
    EXECUTE QUERY;
    
    -- INSERT INTO TABLE WITH MISSING VALUES
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
    )
    INTO tmp_array;
    query := 'INSERT INTO ' || output_table_name || 
             ' SELECT ' || array_to_string(tmp_array, ', ') ||
             ' FROM ' || input_table_name;
    RAISE DEBUG '%', query;

    start_ts := clock_timestamp();
    EXECUTE query;
    end_ts := clock_timestamp();
    RAISE INFO 'INSERT INTO TABLE WITH MISSING VALUES: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
    
    
    ---create indexes
    start_ts := clock_timestamp();
    EXECUTE 'CREATE INDEX idx_is_null_distance ON join_table_complete (distance_isnull) WHERE distance_isnull';
    EXECUTE 'CREATE INDEX idx_is_null_CRS_DEP_HOUR ON join_table_complete (CRS_DEP_HOUR_isnull) WHERE CRS_DEP_HOUR_isnull';
    EXECUTE 'CREATE INDEX idx_is_null_TAXI_OUT ON join_table_complete (TAXI_OUT_isnull) WHERE TAXI_OUT_isnull';
    EXECUTE 'CREATE INDEX idx_is_null_TAXI_IN ON join_table_complete (TAXI_IN_isnull) WHERE TAXI_IN_isnull';
    EXECUTE 'CREATE INDEX idx_is_null_DIVERTED ON join_table_complete (DIVERTED_isnull) WHERE DIVERTED_isnull';
    EXECUTE 'CREATE INDEX idx_is_null_ARR_DELAY ON join_table_complete (ARR_DELAY_isnull) WHERE ARR_DELAY_isnull';
    EXECUTE 'CREATE INDEX idx_is_null_DEP_DELAY ON join_table_complete (DEP_DELAY_isnull) WHERE DEP_DELAY_isnull';
    end_ts := clock_timestamp();
    RAISE INFO 'CREATE INDEX: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

    
    
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
            
            -- COMPUTE COFACTOR WHERE $col IS NOT NULL
            query_null := 'SELECT SUM(to_cofactor(' ||
                    'ARRAY[ ' || array_to_string(continuous_columns, ', ') || ' ]::float8[],' ||
                    'ARRAY[ ' || array_to_string(categorical_columns, ', ') || ' ]::int4[]' ||
                ')) '
            'FROM ' || output_table_name || ' '
            'WHERE ' || col || '_ISNULL;';
            RAISE DEBUG '%', query_null;
            
            start_ts := clock_timestamp();
            EXECUTE query_null INTO STRICT cofactor_null;
            end_ts := clock_timestamp();
            RAISE INFO 'COFACTOR: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
            
            cofactor_global := cofactor_global - cofactor_null;
            
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
                ' WHERE ' || col || '_ISNULL;';
            RAISE DEBUG '%', query;

            start_ts := clock_timestamp();
            EXECUTE query;
            end_ts := clock_timestamp();
            RAISE INFO 'IMPUTE DATA: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
            
            start_ts := clock_timestamp();
            EXECUTE query_null INTO STRICT cofactor_null;
            cofactor_global := cofactor_global + cofactor_null;
            end_ts := clock_timestamp();
            RAISE INFO 'COFACTOR: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
            COMMIT;
        END LOOP;
    END LOOP;
END$$;

-- SET client_min_messages TO INFO;

-- CALL MICE_baseline('R', 'R_complete', ARRAY['A', 'B', 'C', 'D', 'E'], ARRAY[]::text[], ARRAY[ 'A', 'B', 'C'], ARRAY[]::text[]);

-- CALL MICE_baseline('flights_prep.Flight', 'flight_complete', ARRAY['dep_delay', 'taxi_out', 'taxi_in', 'arr_delay', 'diverted', 'actual_elapsed_time', 'air_time', 'dep_time_hour', 'dep_time_min', 'wheels_off_hour', 'wheels_off_min', 'wheels_on_hour', 'wheels_on_min', 'arr_time_hour', 'arr_time_min', 'month_sin', 'month_cos', 'day_sin', 'day_cos', 'weekday_sin', 'weekday_cos', 'extra_day_arr', 'extra_day_dep'], ARRAY[]::text[], ARRAY[ 'dep_delay', 'arr_delay', 'taxi_in', 'taxi_out', 'diverted' ], ARRAY[]::text[]);

---CREATE TEMPORARY TABLE join_table AS (SELECT * FROM flight.Route JOIN flight.schedule USING (ROUTE_ID) JOIN flight.flight USING (SCHEDULE_ID))

CALL MICE_baseline('join_table', 'join_table_complete', ARRAY['dep_delay', 'taxi_out', 'taxi_in', 'arr_delay', 'diverted', 'actual_elapsed_time', 'air_time', 'dep_time_hour', 'dep_time_min', 'wheels_off_hour', 'wheels_off_min', 'wheels_on_hour', 'wheels_on_min', 'arr_time_hour', 'arr_time_min', 'month_sin', 'month_cos', 'day_sin', 'day_cos', 'weekday_sin', 'weekday_cos', 'extra_day_arr', 'extra_day_dep', 'distance', 'crs_dep_hour', 'crs_dep_min', 'crs_arr_hour', 'crs_arr_min' ], ARRAY[]::text[], ARRAY[ 'dep_delay', 'arr_delay', 'taxi_in', 'taxi_out', 'diverted', 'distance', 'crs_dep_hour' ], ARRAY[]::text[]);