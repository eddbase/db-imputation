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
    columns_lower text[];
    cofactor_global cofactor;
    cofactor_null cofactor;
    col text;
    params float8[];
    tmp_columns_names text[];
    label_index int4;
    max_range int4;    
    categorical_uniq_vals_sorted int[] := ARRAY[];
    upper_bound_categorical  int[] := ARRAY[];
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
                 ' FOR VALUES FROM (' || col_id || ') TO (' || col_id + 1 || ') WITH (fillfactor=100)';
        RAISE DEBUG '%', query;
        EXECUTE QUERY;
    END LOOP;
    
    IF max_range >= 2 THEN
    	    if array_length(categorical_columns_null, 1) > 0 then
    			query := 'CREATE UNLOGGED TABLE ' || output_table_name || '_nullcnt2' || 
             	' PARTITION OF ' || output_table_name ||
             	' FOR VALUES FROM (2) TO (' || array_length(continuous_columns_null, 1) + array_length(categorical_columns_null, 1) + 1 || ') WITH (fillfactor=75)';
             else
            	query := 'CREATE UNLOGGED TABLE ' || output_table_name || '_nullcnt2' || 
             	' PARTITION OF ' || output_table_name ||
             	' FOR VALUES FROM (2) TO (' || array_length(continuous_columns_null, 1) + 1 || ') WITH (fillfactor=75)';
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
        FROM unnest(continuous_columns_null || categorical_columns_null) AS x
    ) INTO tmp_array;
    
    SELECT (
        SELECT array_agg(
        		CASE 
        			WHEN array_position(continuous_columns_null, x) IS NULL THEN
        				' WHEN ' || x || ' IS NULL THEN ' || array_position (categorical_columns_null, x) + array_length(continuous_columns_null, 1)
                    ELSE 
                    	' WHEN ' || x || ' IS NULL THEN ' || array_position (continuous_columns_null, x)
                END
        )         
        FROM unnest(continuous_columns_null || categorical_columns_null) AS x
    )
    INTO tmp_array2;    
    
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
    
    FOR i in 1..1 LOOP
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
            label_index := array_position(continuous_columns, col);
            start_ts := clock_timestamp();
            params := ridge_linear_regression(cofactor_global, label_index - 1, 0.001, 0, 10000, 1);
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
                        
            SELECT array_agg('(ARRAY['||array_to_string(params[(array_length(continuous_columns, 1) + a.bound_down + 1 + 1) : (array_length(continuous_columns, 1) + a.bound_up + 1)], ', ')||'])[array_position(ARRAY['||array_to_string(categorical_uniq_vals_sorted[bound_down+1:bound_up], ', ') ||'], '||x||')]')
            FROM unnest(categorical_columns, upper_bound_categorical, low_bound_categorical) WITH ORDINALITY a(x, bound_up, bound_down, nr) 
            INTO tmp_array2;--add categorical to imputation
                        
            --- 'normal_rand(1, 0, ' || sqrt(params[array_length(params, 1)])::text || ')'
            SELECT array_agg(LOWER(x)) FROM unnest(continuous_columns) as x INTO columns_lower;

            
            SELECT array_agg(
                CASE WHEN array_position(columns_lower, LOWER(x)) = label_index THEN
                    array_to_string(array_append(array_prepend(params[1]::text, tmp_array || tmp_array2), '(sqrt(-2 * ln(random()))*cos(2*pi()*random()))*'||sqrt(params[array_length(params, 1)])::text), ' + ') || ' AS ' || x
                ELSE
                    x || ' AS ' || x
                END
                )
            FROM unnest(tmp_columns_names) AS x
            INTO tmp_array3;
            
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
                ' SET ' || col || ' = ' || array_to_string(array_append(array_prepend(params[1]::text, tmp_array || tmp_array2), '(sqrt(-2 * ln(random()))*cos(2*pi()*random()))*'||sqrt(params[array_length(params, 1)])::text), ' + ') ||
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


CALL MICE_incremental_partitioned('join_table', 'join_table_complete', ARRAY['AQI', 'CO', 'O3', 'PM10', 'PM2_5', 'NO2', 'NOx', 'NO_', 'WindSpeed', 'WindDirec', 'SO2_AVG'], ARRAY[]::text[], ARRAY['CO', 'O3', 'PM10', 'PM2_5', 'NO2', 'NOx', 'NO_', 'WindSpeed', 'WindDirec', 'SO2_AVG'], ARRAY[]::text[]);

-- SET client_min_messages TO INFO;
