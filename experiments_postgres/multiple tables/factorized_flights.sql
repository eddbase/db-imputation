CREATE OR REPLACE PROCEDURE MICE_factorized(
    input_table_name text,
    output_table_name text,
    continuous_columns text[],
    continuous_columns_fact text[],
    categorical_columns text[],--make sure this follows the order of the cofactor matrix!
    categorical_columns_fact text[],--make sure this follows the order of the cofactor matrix!
    continuous_columns_null text[],
    categorical_columns_null text[]
)
    LANGUAGE plpgsql AS
$$
DECLARE
    start_ts                     timestamptz;
    end_ts                       timestamptz;
    query                        text;
    query2                       text;
    subquery                     text;
    query_null1                  text;
    query_null2                  text;
    col_averages                 float8[];
    col_mode                     int4[];
    tmp_array                    text[];
    tmp_array2                   text[];
    tmp_array3                   text[];
    cofactor_global              cofactor;
    cofactor_null                cofactor;
    col                          text;
    params                       float8[];
    label_index                  int4;
    max_range                    int4;
    tmp_columns_names			text[];
    columns_lower text[];
    categorical_uniq_vals_sorted int[] := ARRAY[0,1,0,1,0,1,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22];--categorical columns sorted by values  -> FLIGHTS
    upper_bound_categorical  int[] := ARRAY[2,4,6,29];--5,9,11 
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
    end if;

    IF array_length(categorical_columns_null, 1) > 0 THEN
        EXECUTE query2 INTO col_mode;
    end if;
    end_ts := clock_timestamp();


    RAISE DEBUG 'AVERAGES: %', col_averages;
    RAISE INFO 'COMPUTE COLUMN AVERAGES: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

    -- CREATE TABLE WITH MISSING VALUES
    query := 'DROP TABLE IF EXISTS ' || output_table_name;
    RAISE DEBUG '%', query;
    EXECUTE QUERY;
    --EXECUTE 'DROP SEQUENCE IF EXISTS table_name_id_seq'

    SELECT (SELECT array_agg(x || ' float8')
            FROM unnest(continuous_columns_fact) AS x) ||
           (SELECT array_agg(x || ' int')
            FROM unnest(categorical_columns_fact) AS x) ||
           (SELECT array_agg(x || '_ISNULL bool')
            FROM unnest(continuous_columns_null) AS x) ||
           (SELECT array_agg(x || '_ISNULL bool')
            FROM unnest(categorical_columns_null) AS x)
               || (
               ARRAY [ 'NULL_CNT int', 'NULL_COL_ID int', 'ROW_ID serial' ]
               )
    INTO tmp_array;
        
    query := 'CREATE UNLOGGED TABLE ' || output_table_name || '( SCHEDULE_ID INTEGER, ' ||
                array_to_string(tmp_array, ', ')||') PARTITION BY RANGE (NULL_CNT)';
    RAISE DEBUG '%', query;
    EXECUTE QUERY;
    
    COMMIT;
    
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

    FOR col_id in 1..max_range
        LOOP
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
    end if;

    COMMIT;

    -- INSERT INTO TABLE WITH MISSING VALUES
    start_ts := clock_timestamp();
    SELECT (SELECT array_agg(
                           CASE
                               WHEN array_position(continuous_columns_null, x) IS NULL THEN
                                   x
                               ELSE
                                    'COALESCE(' || x || ', ' ||
                                    col_averages[array_position(continuous_columns_null, x)] || ')'
                               END
                       )
            FROM unnest(continuous_columns_fact) AS x) ||
           (SELECT array_agg(
                            CASE
                                WHEN array_position(categorical_columns_null, x) IS NULL THEN
                                    x
                                ELSE
                                    'COALESCE(' || x || ', ' ||
                                    col_mode[array_position(categorical_columns_null, x)] || ')'
                                END
                            )
            FROM unnest(categorical_columns_fact) AS x) ||
           (SELECT array_agg(x || ' IS NULL')
            FROM unnest(continuous_columns_null) AS x) || (SELECT array_agg(x || ' IS NULL')
                                                           FROM unnest(categorical_columns_null) AS x) ||
           (SELECT ARRAY [ array_to_string(array_agg('(CASE WHEN ' || x || ' IS NULL THEN 1 ELSE 0 END)'), ' + ') ]
            FROM unnest(continuous_columns_null || categorical_columns_null) AS x)
    INTO tmp_array;

    SELECT (SELECT array_agg(
                           CASE
                               WHEN array_position(continuous_columns_null, x) IS NULL THEN
                                               ' WHEN ' || x || ' IS NULL THEN ' ||
                                               array_position(categorical_columns_null, x) +
                                               array_length(continuous_columns_null, 1)
                               ELSE
                                               ' WHEN ' || x || ' IS NULL THEN ' ||
                                               array_position(continuous_columns_null, x)
                               END
                       )
            FROM unnest(continuous_columns_null || categorical_columns_null) AS x)
    INTO tmp_array2;


    query := 'INSERT INTO ' || output_table_name ||
             ' SELECT SCHEDULE_ID, ' || array_to_string(tmp_array, ', ') || ', CASE ' || array_to_string(tmp_array2, '  ') ||
             ' ELSE 0 END ' ||
             ' FROM ' || input_table_name;
    RAISE DEBUG '%', query;

    EXECUTE query;
    end_ts := clock_timestamp();
    RAISE INFO 'INSERT INTO TABLE WITH MISSING VALUES: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

    -- COMPUTE MAIN COFACTOR
    
    query := 'SELECT SUM(cnt2*cnt1) FROM '
    || '(SELECT SCHEDULE_ID, SUM(to_cofactor(ARRAY[DEP_DELAY, TAXI_OUT, TAXI_IN, ARR_DELAY, ACTUAL_ELAPSED_TIME, AIR_TIME, DEP_TIME_HOUR, DEP_TIME_MIN, WHEELS_OFF_HOUR, WHEELS_OFF_MIN, WHEELS_ON_HOUR, WHEELS_ON_MIN,  ARR_TIME_HOUR, ARR_TIME_MIN, MONTH_SIN, MONTH_COS, DAY_SIN, DAY_COS, WEEKDAY_SIN, WEEKDAY_COS], ARRAY[DIVERTED, EXTRA_DAY_ARR, EXTRA_DAY_DEP])) AS cnt2 '
    || ' FROM ' || output_table_name
	|| ' group by SCHEDULE_ID) as flight JOIN ('
    || '    SELECT SCHEDULE_ID, (to_cofactor(ARRAY[CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN], ARRAY[OP_CARRIER]::int[]) * cnt) as cnt1'
    || '    FROM flight.schedule as schedule JOIN '
    || '    (SELECT'
    || '    ROUTE_ID, to_cofactor(ARRAY[DISTANCE], ARRAY[]::int[]) as cnt'
    || '    FROM'
    || '      flight.route) as route'
    || '      ON route.ROUTE_ID = schedule.ROUTE_ID) as schedule ON flight.SCHEDULE_ID = schedule.SCHEDULE_ID;';


    RAISE DEBUG '%', query;

    start_ts := clock_timestamp();
    EXECUTE query INTO STRICT cofactor_global;
    end_ts := clock_timestamp();
    RAISE INFO 'COFACTOR global: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

    COMMIT;

    FOR i in 1..1
        LOOP
            RAISE INFO 'Iteration %', i;
            FOREACH col in ARRAY categorical_columns_null
                LOOP
                    RAISE INFO '  |- Column %', col;

                    -- COMPUTE COFACTOR WHERE $col IS NULL
                    
             query_null1 := 'SELECT SUM(cnt2*cnt1) FROM '
              || '(SELECT SCHEDULE_ID, SUM(to_cofactor(ARRAY[DEP_DELAY, TAXI_OUT, TAXI_IN, ARR_DELAY, ACTUAL_ELAPSED_TIME, AIR_TIME, DEP_TIME_HOUR, DEP_TIME_MIN, WHEELS_OFF_HOUR, WHEELS_OFF_MIN, WHEELS_ON_HOUR, WHEELS_ON_MIN,  ARR_TIME_HOUR, ARR_TIME_MIN, MONTH_SIN, MONTH_COS, DAY_SIN, DAY_COS, WEEKDAY_SIN, WEEKDAY_COS], ARRAY[DIVERTED, EXTRA_DAY_ARR, EXTRA_DAY_DEP])) AS cnt2 '
              || ' FROM ' || output_table_name ||' WHERE NULL_CNT = 1 AND NULL_COL_ID = ' || array_position(categorical_columns_null, col) + array_length(continuous_columns_null, 1)
			  || ' group by SCHEDULE_ID) as flight JOIN ('
              || '    SELECT SCHEDULE_ID, (to_cofactor(ARRAY[CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN], ARRAY[OP_CARRIER]) * cnt) as cnt1'
              || '    FROM flight.schedule as schedule JOIN '
              || '    (SELECT'
              || '    ROUTE_ID, to_cofactor(ARRAY[DISTANCE], ARRAY[]::int[]) as cnt'
              || '    FROM'
              || '      flight.route) as route'
              || '      ON route.ROUTE_ID = schedule.ROUTE_ID) as schedule ON flight.SCHEDULE_ID = schedule.SCHEDULE_ID;';
             
             
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
                                        
                    
                    query_null2 := 'SELECT SUM(cnt2*cnt1) FROM '
              || '(SELECT SCHEDULE_ID, SUM(to_cofactor(ARRAY[DEP_DELAY, TAXI_OUT, TAXI_IN, ARR_DELAY, ACTUAL_ELAPSED_TIME, AIR_TIME, DEP_TIME_HOUR, DEP_TIME_MIN, WHEELS_OFF_HOUR, WHEELS_OFF_MIN, WHEELS_ON_HOUR, WHEELS_ON_MIN,  ARR_TIME_HOUR, ARR_TIME_MIN, MONTH_SIN, MONTH_COS, DAY_SIN, DAY_COS, WEEKDAY_SIN, WEEKDAY_COS], ARRAY[DIVERTED, EXTRA_DAY_ARR, EXTRA_DAY_DEP])) AS cnt2 '
              || ' FROM '|| output_table_name ||' WHERE NULL_CNT >= 2 AND '|| col || '_ISNULL '
			  || ' group by SCHEDULE_ID) as flight JOIN ('
              || '    SELECT SCHEDULE_ID, (to_cofactor(ARRAY[CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN], ARRAY[OP_CARRIER]) * cnt) as cnt1'
              || '    FROM flight.schedule as schedule JOIN '
              || '    (SELECT'
              || '    ROUTE_ID, to_cofactor(ARRAY[DISTANCE], ARRAY[]::int[]) as cnt'
              || '    FROM'
              || '      flight.route) as route'
              || '      ON route.ROUTE_ID = schedule.ROUTE_ID) as schedule ON flight.SCHEDULE_ID = schedule.SCHEDULE_ID;';
                    
                    
                    
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
                    RAISE INFO 'TRAIN ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

                    -- IMPUTE

                    --skip current label in categorical features
                    --cat columns: letters

                    low_bound_categorical := ARRAY [0] || array_remove(upper_bound_categorical,
                                                                       upper_bound_categorical[array_upper(upper_bound_categorical, 1)]);
                    --offset wrong when label skipped
                    RAISE DEBUG ' cat columns 3: %',categorical_columns;
                    RAISE DEBUG ' cat columns null 3: %', categorical_columns_null;
                    
                    IF array_length(categorical_columns, 1) = 1 AND
                       array_length(categorical_columns_null, 1) = 1 THEN --only 1 cat. var. null, so no cat. features
                        subquery := 'ARRAY[]';
                    ELSE
	           			subquery := '(SELECT array_agg(array_position((ARRAY[' || array_to_string(categorical_uniq_vals_sorted, ' ,') || '])[bound_down+1 : bound_up], a.elem) - 1 + bound_down - CASE WHEN a.nr < ' || label_index || ' THEN 0 ELSE ' || upper_bound_categorical[label_index]-low_bound_categorical[label_index] ||' END)' ||
    	        		' FROM unnest(ARRAY[' || array_to_string(categorical_columns, ', ') || ']::int[], ARRAY[' || array_to_string(upper_bound_categorical , ', ') || ']::int[], ARRAY[' || array_to_string(low_bound_categorical , ', ') ||']::int[]) WITH ORDINALITY a(elem, bound_up, bound_down, nr) ' ||
       		 			' WHERE a.nr != ' || label_index ||')';
                    END IF;

                    RAISE DEBUG ' SUBQUERY: %', subquery;
                    
                    query := 'SELECT array_agg(quote_ident(column_name)) FROM information_schema.columns WHERE table_name = ''' || output_table_name || ''';';
            		EXECUTE query INTO tmp_columns_names;
            		RAISE DEBUG '%', tmp_columns_names;
            		
            		query := 'ALTER TABLE '||output_table_name || '_nullcnt1 DETACH PARTITION '|| output_table_name ||'_nullcnt1_col'||array_position(categorical_columns_null, col)+ array_length(continuous_columns_null, 1);
            		RAISE DEBUG '%', query;
            		EXECUTE query;
            		query := 'ALTER TABLE '||output_table_name ||'_nullcnt1_col'||array_position(categorical_columns_null, col)+ array_length(continuous_columns_null, 1)  ||' RENAME TO tmp_table';            
            		RAISE DEBUG '%', query;
            		EXECUTE query;
            		SELECT array_agg(LOWER(x)) FROM unnest(categorical_columns) as x INTO columns_lower;

            		
            		SELECT array_agg(
            			CASE WHEN array_position(columns_lower, LOWER(x)) = label_index THEN
            	    		'(ARRAY[' || array_to_string(categorical_uniq_vals_sorted[low_bound_categorical[label_index]+1 : upper_bound_categorical[label_index]], ', ') ||'])[1+ lda_impute(ARRAY[ ' || array_to_string(params, ', ') || ']::float4[], ' || 'ARRAY[ ' || array_to_string(continuous_columns, ', ') || ' ]::float4[], '
                			|| subquery || ' ::int[])] ' || ' AS ' || x
                		WHEN x = 'schedule_id' THEN
                			'flight.' || x || ' AS ' || x
            			ELSE
            	    		x || ' AS ' || x
            			END
            		)
            		FROM unnest(tmp_columns_names) AS x
            		INTO tmp_array3;
            		
            		query := 'CREATE UNLOGGED TABLE ' || output_table_name || '_nullcnt1_col'||array_position(categorical_columns_null, col)+ array_length(continuous_columns_null, 1)  || ' AS SELECT ';
            		query := query || array_to_string(tmp_array3, ' , ') ||' FROM ';
            		query := query ||' ( SELECT * FROM tmp_table) as flight JOIN ('
                    || '    SELECT SCHEDULE_ID, CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN, OP_CARRIER, route.DISTANCE '
                    || '    FROM flight.schedule as schedule JOIN flight.route'
                    || '      ON route.ROUTE_ID = schedule.ROUTE_ID) as schedule ON flight.SCHEDULE_ID = schedule.SCHEDULE_ID;';
            		
                    RAISE DEBUG '%', query;
            		EXECUTE query;
        			query := 'ALTER TABLE '|| output_table_name || '_nullcnt1_col'||array_position(categorical_columns_null, col) + array_length(continuous_columns_null, 1)  ||' ALTER COLUMN row_id SET NOT NULL';
        	        RAISE DEBUG '%', query;
                    EXECUTE query;
            		query := 'ALTER TABLE '|| output_table_name || '_nullcnt1 ATTACH PARTITION '||output_table_name || '_nullcnt1_col'||array_position(categorical_columns_null, col)+ array_length(continuous_columns_null, 1)  ||' FOR VALUES FROM (' || array_position(categorical_columns_null, col)+ array_length(continuous_columns_null, 1)  || ') TO (' || array_position(categorical_columns_null, col)+ array_length(continuous_columns_null, 1)  + 1 || ')';
                    RAISE DEBUG '%', query;
                    EXECUTE query;
                    EXECUTE 'DROP TABLE tmp_table';
            		
                    RAISE INFO 'IMPUTE DATA (1): ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

                    query := 'UPDATE ' || output_table_name ||
                             ' SET ' || col || ' = (ARRAY[' || array_to_string(categorical_uniq_vals_sorted[low_bound_categorical[label_index]+1 : upper_bound_categorical[label_index]], ', ') ||'])[1+ lda_impute(ARRAY[ ' || array_to_string(params, ', ') || ']::float4[], ' || 'ARRAY[ ' || array_to_string(continuous_columns, ', ') || ' ]::float4[], '
                			|| subquery || ' ::int[])] FROM (SELECT * FROM flight.schedule AS schedule JOIN flight.route AS route ON route.ROUTE_ID = schedule.ROUTE_ID) AS subquery
                              WHERE '||output_table_name||'.NULL_CNT >= 2 AND ' ||output_table_name||'.'|| col || '_ISNULL AND subquery.SCHEDULE_ID = '||output_table_name||'.SCHEDULE_ID;';
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


            FOREACH col in ARRAY continuous_columns_null
                LOOP
                    RAISE INFO '  |- Column %', col;

                    -- COMPUTE COFACTOR WHERE $col IS NULL
  
               query_null1 := 'SELECT SUM(cnt2*cnt1) FROM '
              || '(SELECT SCHEDULE_ID, SUM(to_cofactor(ARRAY[DEP_DELAY, TAXI_OUT, TAXI_IN, ARR_DELAY, ACTUAL_ELAPSED_TIME, AIR_TIME, DEP_TIME_HOUR, DEP_TIME_MIN, WHEELS_OFF_HOUR, WHEELS_OFF_MIN, WHEELS_ON_HOUR, WHEELS_ON_MIN,  ARR_TIME_HOUR, ARR_TIME_MIN, MONTH_SIN, MONTH_COS, DAY_SIN, DAY_COS, WEEKDAY_SIN, WEEKDAY_COS], ARRAY[DIVERTED, EXTRA_DAY_ARR, EXTRA_DAY_DEP])) AS cnt2 '
              || ' FROM '|| output_table_name ||' WHERE NULL_CNT = 1 AND NULL_COL_ID = ' || array_position(continuous_columns_null, col)
			  || ' group by SCHEDULE_ID) as flight JOIN ('
              || '    SELECT SCHEDULE_ID, (to_cofactor(ARRAY[CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN], ARRAY[OP_CARRIER]) * cnt) as cnt1'
              || '    FROM flight.schedule as schedule JOIN '
              || '    (SELECT'
              || '    ROUTE_ID, to_cofactor(ARRAY[DISTANCE], ARRAY[]::int[]) as cnt'
              || '    FROM'
              || '      flight.route) as route'
              || '      ON route.ROUTE_ID = schedule.ROUTE_ID) as schedule ON flight.SCHEDULE_ID = schedule.SCHEDULE_ID;';
              
              
                    RAISE DEBUG '%', query_null1;

                    start_ts := clock_timestamp();
                    EXECUTE query_null1 INTO STRICT cofactor_null;
                    end_ts := clock_timestamp();
                    RAISE INFO 'COFACTOR NULL (1): ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

                    IF cofactor_null IS NOT NULL THEN
                        cofactor_global := cofactor_global - cofactor_null;
                    END IF;

                    query_null2 := 'SELECT SUM(cnt2*cnt1) FROM '
              || '(SELECT SCHEDULE_ID, SUM(to_cofactor(ARRAY[DEP_DELAY, TAXI_OUT, TAXI_IN, ARR_DELAY, ACTUAL_ELAPSED_TIME, AIR_TIME, DEP_TIME_HOUR, DEP_TIME_MIN, WHEELS_OFF_HOUR, WHEELS_OFF_MIN, WHEELS_ON_HOUR, WHEELS_ON_MIN,  ARR_TIME_HOUR, ARR_TIME_MIN, MONTH_SIN, MONTH_COS, DAY_SIN, DAY_COS, WEEKDAY_SIN, WEEKDAY_COS], ARRAY[DIVERTED, EXTRA_DAY_ARR, EXTRA_DAY_DEP])) AS cnt2 '
              || ' FROM '|| output_table_name ||' WHERE NULL_CNT >= 2 AND '|| col || '_ISNULL'
			  || ' group by SCHEDULE_ID) as flight JOIN ('
              || '    SELECT SCHEDULE_ID, (to_cofactor(ARRAY[CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN], ARRAY[OP_CARRIER]) * cnt) as cnt1'
              || '    FROM flight.schedule as schedule JOIN '
              || '    (SELECT'
              || '    ROUTE_ID, to_cofactor(ARRAY[DISTANCE], ARRAY[]::int[]) as cnt'
              || '    FROM'
              || '      flight.route) as route'
              || '      ON route.ROUTE_ID = schedule.ROUTE_ID) as schedule ON flight.SCHEDULE_ID = schedule.SCHEDULE_ID;';



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
                    params := ridge_linear_regression(cofactor_global, label_index - 1, 0.001, 0, 10000, 1);
                    end_ts := clock_timestamp();
                    RAISE INFO 'TRAIN: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
                    RAISE DEBUG '%', params;
                    
                    
                    --impute                    
                    query := 'SELECT array_agg(quote_ident(column_name)) FROM information_schema.columns WHERE table_name = ''' || output_table_name || ''';';
            		EXECUTE query INTO tmp_columns_names;
            		RAISE DEBUG '%', tmp_columns_names;
            		
            		-- IMPUTE
                    SELECT array_agg(x || ' * ' || params[array_position(continuous_columns, x) + 1])
                    FROM unnest(continuous_columns) AS x
                    WHERE array_position(continuous_columns, x) != label_index
                    INTO tmp_array;

                    
                    low_bound_categorical := ARRAY[0] || array_remove(upper_bound_categorical , upper_bound_categorical[array_upper(upper_bound_categorical, 1)]);
                        
            		SELECT array_agg('(ARRAY['||array_to_string(params[(array_length(continuous_columns, 1) + a.bound_down + 1 + 1) : (array_length(continuous_columns, 1) + a.bound_up + 1)], ', ')||'])[array_position(ARRAY['||array_to_string(categorical_uniq_vals_sorted[bound_down+1:bound_up], ', ') ||'], '||x||')]')
            		FROM unnest(categorical_columns, upper_bound_categorical, low_bound_categorical) WITH ORDINALITY a(x, bound_up, bound_down, nr) 
            		INTO tmp_array2;--add categorical to imputation

                    SELECT array_agg(LOWER(x)) FROM unnest(continuous_columns) as x INTO columns_lower;
                    SELECT array_agg(
                    	CASE WHEN array_position(columns_lower, LOWER(x)) = label_index THEN
                    		array_to_string(array_append(array_prepend(params[1]::text, tmp_array || tmp_array2), 'random()*'||sqrt(params[array_length(params, 1)])::text), ' + ') || ' AS ' || x
                    	WHEN x = 'schedule_id' THEN
                			'flight.' || x || ' AS ' || x
                		ELSE
                    		x || ' AS ' || x
                		END
                		)
            		FROM unnest(tmp_columns_names) AS x
            		INTO tmp_array3;

                    
                    start_ts := clock_timestamp();

                    query := 'ALTER TABLE '||output_table_name || '_nullcnt1 DETACH PARTITION '|| output_table_name ||'_nullcnt1_col'||array_position(continuous_columns_null, col);
                    RAISE DEBUG '%', query;
                    EXECUTE query;
                    query := 'ALTER TABLE '||output_table_name ||'_nullcnt1_col'||array_position(continuous_columns_null, col) ||' RENAME TO tmp_table';
                    RAISE DEBUG '%', query;
                    EXECUTE query;
                    query := 'CREATE UNLOGGED TABLE ' || output_table_name || '_nullcnt1_col'||array_position(continuous_columns_null, col) || ' AS ';
                    query := query || ' SELECT ' || array_to_string(tmp_array3, ' , ') || ' FROM ';                    
            		query := query ||' ( SELECT * FROM tmp_table) as flight JOIN ('
                    || '    SELECT SCHEDULE_ID, CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN, OP_CARRIER, route.DISTANCE '
                    || '    FROM flight.schedule as schedule JOIN flight.route'
                    || '      ON route.ROUTE_ID = schedule.ROUTE_ID) as schedule ON flight.SCHEDULE_ID = schedule.SCHEDULE_ID;';
                    
                                        
                    RAISE DEBUG '%', query;
                    EXECUTE query;
                    EXECUTE 'ALTER TABLE '|| output_table_name || '_nullcnt1_col'||array_position(continuous_columns_null, col) ||' ALTER COLUMN row_id SET NOT NULL';
                    query := 'ALTER TABLE '|| output_table_name || '_nullcnt1 ATTACH PARTITION '||output_table_name || '_nullcnt1_col'||array_position(continuous_columns_null, col) ||' FOR VALUES FROM (' || array_position(continuous_columns_null, col) || ') TO (' || array_position(continuous_columns_null, col) + 1 || ')';
                    RAISE DEBUG '%', query;
                    EXECUTE query;
                    EXECUTE 'DROP TABLE tmp_table';
                    
                    end_ts := clock_timestamp();
                    RAISE INFO 'IMPUTE DATA (1): ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
                    
                    
                    
                    query := 'UPDATE ' || output_table_name || ' SET ' || col || ' = ' || array_to_string(array_append(array_prepend(params[1]::text, tmp_array || tmp_array2), 'random()*'||sqrt(params[array_length(params, 1)])::text), ' + ') ||
                			' FROM (SELECT * FROM flight.schedule AS schedule JOIN flight.route AS route ON route.ROUTE_ID = schedule.ROUTE_ID) AS subquery
                              WHERE '||output_table_name||'.NULL_CNT >= 2 AND ' ||output_table_name||'.'|| col || '_ISNULL AND subquery.SCHEDULE_ID = '||output_table_name||'.SCHEDULE_ID;';
                             
                    
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
END
$$;

CALL MICE_factorized('flight.flight', 'output_flight',
                     ARRAY ['DEP_DELAY', 'TAXI_OUT', 'TAXI_IN', 'ARR_DELAY',
                                                 'ACTUAL_ELAPSED_TIME', 'AIR_TIME', 'DEP_TIME_HOUR', 'DEP_TIME_MIN',
                                                 'WHEELS_OFF_HOUR', 'WHEELS_OFF_MIN', 'WHEELS_ON_HOUR', 'WHEELS_ON_MIN',
                                                 'ARR_TIME_HOUR', 'ARR_TIME_MIN', 'MONTH_SIN', 'MONTH_COS', 'DAY_SIN',
                                                 'DAY_COS', 'WEEKDAY_SIN', 'WEEKDAY_COS', 'CRS_DEP_HOUR', 'CRS_DEP_MIN', 'CRS_ARR_HOUR', 'CRS_ARR_MIN',
                                                 'DISTANCE'],
                     ARRAY ['DEP_DELAY', 'TAXI_OUT', 'TAXI_IN', 'ARR_DELAY', 'ACTUAL_ELAPSED_TIME', 'AIR_TIME', 'DEP_TIME_HOUR', 'DEP_TIME_MIN',
                                                 'WHEELS_OFF_HOUR', 'WHEELS_OFF_MIN', 'WHEELS_ON_HOUR', 'WHEELS_ON_MIN',
                                                 'ARR_TIME_HOUR', 'ARR_TIME_MIN', 'MONTH_SIN', 'MONTH_COS', 'DAY_SIN',
                                                 'DAY_COS', 'WEEKDAY_SIN', 'WEEKDAY_COS']::text[],
                     ARRAY['DIVERTED', 'EXTRA_DAY_ARR', 'EXTRA_DAY_DEP', 'OP_CARRIER'],
                     ARRAY ['DIVERTED', 'EXTRA_DAY_ARR', 'EXTRA_DAY_DEP']::text[],
                    ARRAY ['DEP_DELAY', 'TAXI_OUT', 'TAXI_IN', 'ARR_DELAY', 'WHEELS_OFF_HOUR', 'WHEELS_ON_HOUR']::text[],
                    ARRAY ['DIVERTED']::text[]
                        );

--SET client_min_messages TO DEBUG;
