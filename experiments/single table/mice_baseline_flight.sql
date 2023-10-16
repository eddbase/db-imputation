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
    query2 text;
    subquery text;
    query_null1 text;
    query_null2 text;
    col_averages float8[];
    tmp_array text[];
    tmp_array2 text[];
    col_mode int4[];
    cofactor_global cofactor;
    cofactor_null cofactor;
    col text;
    params float4[];
    label_index int4;
    categorical_uniq_vals_sorted int[] := ARRAY[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22, 0,1,0,1,0,1];--categorical columns sorted by values  -> FLIGHTS
    upper_bound_categorical  int[] := ARRAY[23,25,27,29];--5,9,11 
    columns_lower text[];
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
    INTO tmp_array;
        
    query := 'CREATE UNLOGGED TABLE ' || output_table_name || '( ' ||
                array_to_string(tmp_array, ', ') || ', ROW_ID serial) WITH (fillfactor=15)';
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

    start_ts := clock_timestamp();
    EXECUTE query;
    end_ts := clock_timestamp();
    RAISE INFO 'INSERT INTO TABLE WITH MISSING VALUES: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
    COMMIT;

    FOR i in 1..1 LOOP
        RAISE INFO 'Iteration %', i;

        FOREACH col in ARRAY categorical_columns_null LOOP
            RAISE INFO '  |- Column %', col;
            
            -- COMPUTE COFACTOR WHERE $col IS NOT NULL
            query := 'SELECT SUM(to_cofactor(' ||
                    'ARRAY[ ' || array_to_string(continuous_columns, ', ') || ' ]::float8[],' ||
                    'ARRAY[ ' || array_to_string(categorical_columns, ', ') || ' ]::int4[]' ||
                ')) '
            'FROM ' || output_table_name || ' '
            'WHERE NOT ' || col || '_ISNULL;';
            RAISE DEBUG '%', query;
            
            start_ts := clock_timestamp();
            EXECUTE query INTO STRICT cofactor_global;
            end_ts := clock_timestamp();
            RAISE INFO 'COFACTOR: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
            
            ---here start...
            
            label_index := array_position(categorical_columns, col);
            RAISE DEBUG 'LABEL INDEX %', label_index;
            start_ts := clock_timestamp();
            params := lda_train(cofactor_global, label_index - 1, 0);
            end_ts := clock_timestamp();
            RAISE DEBUG '%', params;
            RAISE INFO 'TRAIN ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
            
            -- IMPUTE
                        
            low_bound_categorical := ARRAY[0] || array_remove(upper_bound_categorical , upper_bound_categorical[array_upper(upper_bound_categorical, 1)]);
            --offset wrong when label skipped
            IF array_length(categorical_columns,1) = 1 AND array_length(categorical_columns_null,1) = 1 THEN --only 1 cat. var. null, so no cat. features
            	subquery := 'ARRAY[]';
            ELSE
	            subquery := '(SELECT array_agg(array_position((ARRAY[' || array_to_string(categorical_uniq_vals_sorted, ' ,') || '])[bound_down+1 : bound_up], a.elem) - 1 + bound_down - CASE WHEN a.nr < ' || label_index || ' THEN 0 ELSE ' || upper_bound_categorical[label_index]-low_bound_categorical[label_index] ||' END)' ||
    	        ' FROM unnest(ARRAY[' || array_to_string(categorical_columns, ', ') || ']::int[], ARRAY[' || array_to_string(upper_bound_categorical , ', ') || ']::int[], ARRAY[' || array_to_string(low_bound_categorical , ', ') ||']::int[]) WITH ORDINALITY a(elem, bound_up, bound_down, nr) ' ||
       		 	' WHERE a.nr != ' || label_index ||')';
       		 	
       		 	subquery := 'ARRAY['|| array_to_string(categorical_columns[:label_index-1] || categorical_columns[label_index+1 :], ', ') ||']';

       		 	
       		END IF;

            RAISE DEBUG ' %', subquery;
            
                        
            query := 'UPDATE ' || output_table_name || 
                ' SET ' || col || ' = (ARRAY[' || array_to_string(categorical_uniq_vals_sorted[low_bound_categorical[label_index]+1 : upper_bound_categorical[label_index]], ', ') ||'])[1+ lda_predict(ARRAY[ ' || array_to_string(params, ', ') || ']::float4[], ' || 'ARRAY[ ' || array_to_string(continuous_columns, ', ') || ' ]::float4[], '
                || subquery || ' ::int[])] WHERE ' ||col||'_ISNULL';
            RAISE DEBUG 'UPDATE QUERY: %', query;

            start_ts := clock_timestamp();
            EXECUTE query;
            end_ts := clock_timestamp();
            RAISE INFO 'IMPUTE DATA: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
            
            COMMIT;

        END LOOP;
        
        FOREACH col in ARRAY continuous_columns_null LOOP
            RAISE INFO '  |- Column %', col;
            
            -- COMPUTE COFACTOR WHERE $col IS NOT NULL
            query := 'SELECT SUM(to_cofactor(' ||
                    'ARRAY[ ' || array_to_string(continuous_columns, ', ') || ' ]::float8[],' ||
                    'ARRAY[ ' || array_to_string(categorical_columns, ', ') || ' ]::int4[]' ||
                ')) '
            'FROM ' || output_table_name || ' '
            'WHERE NOT ' || col || '_ISNULL;';
            RAISE DEBUG '%', query;
            
            start_ts := clock_timestamp();
            EXECUTE query INTO STRICT cofactor_global;
            end_ts := clock_timestamp();
            RAISE INFO 'COFACTOR: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
            
            ---here start...
            
            label_index := array_position(continuous_columns_null, col);
            params := ridge_linear_regression(cofactor_global, label_index - 1, 0.001, 0, 10000, 1);
                                    
            -- IMPUTE
                        
            SELECT array_agg(x || ' * ' || params[array_position(continuous_columns, x) + 1])
            FROM unnest(continuous_columns) AS x
            WHERE array_position(continuous_columns, x) != label_index
            INTO tmp_array;
            
                        
            SELECT array_agg('(ARRAY['||array_to_string(params[(array_length(continuous_columns, 1) + a.bound_down + 1 + 1) : (array_length(continuous_columns, 1) + a.bound_up + 1)], ', ')||'])[array_position(ARRAY['||array_to_string(categorical_uniq_vals_sorted[bound_down+1:bound_up], ', ') ||'], '||x||')]')
            FROM unnest(categorical_columns, upper_bound_categorical, low_bound_categorical) WITH ORDINALITY a(x, bound_up, bound_down, nr)
            INTO tmp_array2;--add categorical to imputation


            RAISE DEBUG ' %', subquery;
            RAISE DEBUG 'variance %', params[array_length(params, 1)];
            RAISE DEBUG 'last param %', params[array_length(params, 1)-1];
                        
            query := 'UPDATE ' || output_table_name || 
                ' SET ' || col || ' = ' || array_to_string(array_append(array_prepend(params[1]::text, tmp_array || tmp_array2), '(sqrt(-2 * ln(random()))*cos(2*pi()*random()))*'||sqrt(params[array_length(params, 1)])::text), ' + ') ||
                ' WHERE ' || col || '_ISNULL';
            RAISE DEBUG '%', query;

            start_ts := clock_timestamp();
            EXECUTE query;
            end_ts := clock_timestamp();
            RAISE INFO 'IMPUTE DATA: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
            
            COMMIT;

        END LOOP;
        
    END LOOP;
    
END$$;

CALL MICE_baseline('join_table', 'join_table_complete', ARRAY['CRS_DEP_HOUR', 'CRS_DEP_MIN', 'CRS_ARR_HOUR', 'CRS_ARR_MIN', 'DISTANCE', 'DEP_DELAY', 'TAXI_OUT', 'TAXI_IN', 'ARR_DELAY', 'ACTUAL_ELAPSED_TIME', 'AIR_TIME', 'DEP_TIME_HOUR', 'DEP_TIME_MIN', 'WHEELS_OFF_HOUR', 'WHEELS_OFF_MIN', 'WHEELS_ON_HOUR', 'WHEELS_ON_MIN', 'ARR_TIME_HOUR', 'ARR_TIME_MIN', 'MONTH_SIN', 'MONTH_COS', 'DAY_SIN', 'DAY_COS', 'WEEKDAY_SIN', 'WEEKDAY_COS'], ARRAY['OP_CARRIER', 'DIVERTED', 'EXTRA_DAY_DEP', 'EXTRA_DAY_ARR']::text[], ARRAY['WHEELS_ON_HOUR', 'WHEELS_OFF_HOUR', 'TAXI_OUT', 'TAXI_IN', 'ARR_DELAY', 'DEP_DELAY'], ARRAY['DIVERTED']::text[]);

SET client_min_messages TO DEBUG;
