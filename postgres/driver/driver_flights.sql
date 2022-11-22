--computes average for each col_nan

create type t_info as (name text, schema text, columns_no_null text[], columns_null text[], identifier text[], join_keys text[]);

---generate averages for every column (no cat.)
CREATE OR REPLACE FUNCTION standard_imputation(tbl t_info) RETURNS float8[] AS $$
  DECLARE
    _start_ts timestamptz;
    _end_ts timestamptz;
    col text;
    columns text[];
    avg_imputations float8[];
    query text := 'SELECT ARRAY[';
  BEGIN
   _start_ts := clock_timestamp();
    FOREACH col IN ARRAY tbl.columns_null
    LOOP
        query := query || 'AVG(CASE WHEN %s IS NOT NULL THEN %s END), ';
        columns := columns || col || col;
    end LOOP;
    query := RTRIM(query, ', ') || '] FROM %s%s%s;';

    RAISE NOTICE '%', tbl.name;
    RAISE NOTICE '%', format(query, VARIADIC columns || tbl.schema || '{.}' || tbl.name);

    EXECUTE format(query, VARIADIC columns || tbl.schema || '{.}' || tbl.name) INTO avg_imputations;

   RETURN avg_imputations;
  END;
$$ LANGUAGE plpgsql;

--generate cofactor entry function
--create table with rows only where column is null
CREATE OR REPLACE FUNCTION create_table_missing_values(tbl t_info, col_nan text) RETURNS void AS $$
  DECLARE
    col text;
    columns_no_null text[];
    insert_query text := 'CREATE TEMPORARY TABLE tmp_%s_%s AS SELECT ';
  BEGIN
    columns_no_null := array_remove(tbl.columns_null || tbl.columns_no_null,  col_nan);
    FOREACH col IN ARRAY columns_no_null || tbl.identifier || tbl.join_keys
    LOOP
        insert_query := insert_query || '%s, ';
    end loop;
    insert_query := RTRIM(insert_query, ', ') || ' FROM %s WHERE %s IS NULL';
    EXECUTE format('DROP TABLE IF EXISTS tmp_%s_%s', tbl.name, col_nan);
    RAISE NOTICE 'create_table_missing_values:  %', format(insert_query, VARIADIC ARRAY[tbl.name, col_nan] || columns_no_null || tbl.identifier || tbl.join_keys || ARRAY[(tbl.schema || '.' || tbl.name), col_nan]);
    EXECUTE format(insert_query, VARIADIC ARRAY[tbl.name, col_nan] || columns_no_null || tbl.identifier || tbl.join_keys || ARRAY[(tbl.schema || '.' || tbl.name), col_nan]);
  END;
$$ LANGUAGE plpgsql;

--table id, imputed value where original value is null (for every col null)
CREATE OR REPLACE FUNCTION create_table_imputed_values(tbl t_info, col_nan text, average float8) RETURNS void AS $$
  DECLARE
      col text;
    insert_query text := 'CREATE TEMPORARY TABLE tmp_imputed_%s AS SELECT ';
  BEGIN

    FOREACH col IN ARRAY tbl.identifier
    LOOP
        insert_query := insert_query || '%s, ';
    end loop;
    insert_query := insert_query || '%s AS %s FROM tmp_%s_%s';
    EXECUTE format('DROP TABLE IF EXISTS tmp_imputed_%s', col_nan);
    RAISE NOTICE 'create_table_imputed_values:  %', format(insert_query, VARIADIC ARRAY[col_nan] || tbl.identifier || average::text || col_nan || tbl.name || col_nan);
    EXECUTE format(insert_query, VARIADIC ARRAY[col_nan] || tbl.identifier || average::text || col_nan || tbl.name || col_nan);
  END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION impute(tables t_info[], iterations int) RETURNS int AS $$
  DECLARE
    _triple cofactor;
    _triple_removal cofactor;
    _triple_train cofactor;
    _label_idx int;
    col text;
    join_key text;
      group_by text := '';
      index int = 1;
      cnt int;
      queries text[] := ARRAY[]::text[];
      query text;
        query_cof_diff text;
    --no_id_columns_no_null text[] := array_remove(columns_no_null, identifier);
    --_columns text[] := _nan_cols || columns_no_null;--always null || no null
    _params float8[];-- := array[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0];
    _imputed_data float8[];
   _start_ts timestamptz;
   _end_ts   timestamptz;
    tbl t_info;
    cofactor_partial_query text[];
  BEGIN
      ---query follow table order, col nulls - col no nulls
    FOREACH tbl IN ARRAY tables
    LOOP
        group_by := '';
        if tbl.name = 'distances' then
            group_by := group_by || ' airports, ';
        elsif tbl.name = 'schedule' then
            group_by := group_by || ' flight, airports, ';
        elsif tbl.name = 'flights' then
            group_by := group_by || ' flight, ';
        end if;

        group_by := rtrim(group_by, ', ');
        queries := queries || ('(SELECT ' || group_by || ', SUM(to_cofactor(ARRAY[');
        _imputed_data := standard_imputation(tbl);
        ---create tmp table missing values and imputed values
        FOR _counter in 1..cardinality(tbl.columns_null)
        LOOP
            perform create_table_missing_values(tbl,  tbl.columns_null[_counter]);
            perform create_table_imputed_values(tbl, tbl.columns_null[_counter], _imputed_data[_counter]);
            queries[index] := queries[index] || format('COALESCE(%s, %s), ', tbl.columns_null[_counter], _imputed_data[_counter]);
        end loop;
        --queries[index] := rtrim(queries[index], ', ') || '], ARRAY[';

        FOR _counter in 1..cardinality(tbl.columns_no_null)
        LOOP
            queries[index] := queries[index] || format('%s, ', tbl.columns_no_null[_counter]);
        end loop;
        queries[index] := rtrim(queries[index], ', ') || '], ARRAY[]::int[]))';

        ---add group by
        queries[index] := queries[index] || format(' AS cof_%s FROM %s.%s GROUP BY %s) AS %s', index, tbl.schema, tbl.name, group_by, tbl.name);
        index = index + 1;
    END LOOP;

    --distances, schedule, flight
    query := format('SELECT flight, SUM(distances.cof_1 * schedule.cof_2) AS cof_3 FROM %s JOIN %s ON distances.airports = schedule.airports GROUP BY flight) AS join_1', queries[1], queries[2]);
    query := format('SELECT SUM(join_1.cof_3 * flights.cof_3) FROM (%s JOIN %s ON join_1.flight = flights.flight', query, queries[3]);

    RAISE NOTICE '%', query;
    _start_ts := clock_timestamp();
    EXECUTE query into _triple;
    _end_ts   := clock_timestamp();
    RAISE NOTICE 'Preparation: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));

    for counter in 1..iterations
    loop
        --foreach tbl in array tables
        --loop
                ---distance col
                RAISE NOTICE 'Imputing distance';
                _start_ts := clock_timestamp();
                query_cof_diff := 'SELECT SUM(join_1.cof_3 * flights.cof_3) FROM
(SELECT flight, SUM(distances.cof_1*schedule.cof_2) AS cof_3 FROM
(SELECT tmp_distances_DISTANCE.airports, SUM(cont_to_cofactor(tmp_imputed_distance.distance)) as cof_1
FROM tmp_distances_DISTANCE JOIN tmp_imputed_distance ON tmp_distances_DISTANCE.airports = tmp_imputed_distance.airports GROUP BY tmp_distances_DISTANCE.airports) as distances
JOIN
(SELECT flight.schedule.flight, flight.schedule.airports,
SUM(to_cofactor(ARRAY[COALESCE(flight.schedule.CRS_DEP_HOUR, tmp_imputed_CRS_DEP_HOUR.CRS_DEP_HOUR), (CRS_DEP_MIN), (CRS_ARR_HOUR), (CRS_ARR_MIN)], ARRAY[]::int[])) as cof_2
FROM
flight.schedule
LEFT OUTER JOIN tmp_imputed_CRS_DEP_HOUR ON flight.schedule.flight = tmp_imputed_CRS_DEP_HOUR.flight GROUP BY flight.schedule.flight, flight.schedule.airports) as schedule
ON distances.airports = schedule.airports GROUP BY schedule.flight
) as join_1
JOIN
(
SELECT flight.flights.flight, SUM(to_cofactor(ARRAY[COALESCE(flight.flights.TAXI_OUT, tmp_imputed_TAXI_OUT.TAXI_OUT), (COALESCE(flight.flights.TAXI_IN, tmp_imputed_TAXI_IN.TAXI_IN)), (COALESCE(flight.flights.DIVERTED, tmp_imputed_DIVERTED.DIVERTED)), (COALESCE(flight.flights.ARR_DELAY, tmp_imputed_ARR_DELAY.ARR_DELAY)), (COALESCE(flight.flights.DEP_DELAY, tmp_imputed_DEP_DELAY.DEP_DELAY)), (ACTUAL_ELAPSED_TIME), (AIR_TIME), (DEP_TIME_HOUR), (DEP_TIME_MIN), (WHEELS_OFF_HOUR), (WHEELS_OFF_MIN), (WHEELS_ON_HOUR), (WHEELS_ON_MIN), (ARR_TIME_HOUR), (ARR_TIME_MIN), (MONTH_SIN), (MONTH_COS), (DAY_SIN), (DAY_COS), (WEEKDAY_SIN), (WEEKDAY_COS), (EXTRA_DAY_ARR), (EXTRA_DAY_DEP)], ARRAY[]::int[])) AS cof_3 FROM flight.flights
LEFT OUTER JOIN tmp_imputed_TAXI_OUT ON flight.flights.id = tmp_imputed_TAXI_OUT.id
LEFT OUTER JOIN tmp_imputed_TAXI_IN ON flight.flights.id = tmp_imputed_TAXI_IN.id
LEFT OUTER JOIN tmp_imputed_DIVERTED ON flight.flights.id = tmp_imputed_DIVERTED.id
LEFT OUTER JOIN tmp_imputed_ARR_DELAY ON flight.flights.id = tmp_imputed_ARR_DELAY.id
LEFT OUTER JOIN tmp_imputed_DEP_DELAY ON flight.flights.id = tmp_imputed_DEP_DELAY.id
 GROUP BY flight.flights.flight
) as flights ON join_1.flight = flights.flight';
                execute(query_cof_diff) INTO _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Generate cofactor of nan values: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _triple_train := _triple - _triple_removal;
                -- fit model over cofactor matrix
                _label_idx := 0;--array_position(_columns, col)-1;--c code

                _start_ts := clock_timestamp();
                _params := ridge_linear_regression(_triple_train, _label_idx, 0.001, 0, 1000);

                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Train the model: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));

                --update cofactor matrix and table
                _start_ts := clock_timestamp();
                EXECUTE 'TRUNCATE TABLE tmp_imputed_distance';

                query := 'INSERT INTO tmp_imputed_DISTANCE (SELECT schedule.airports, SUM(schedule.val + flights.val) FROM
(SELECT tmp_distances_DISTANCE.airports, flight.schedule.flight, ('||_params[1]::text || ')+SUM('|| _params[3]::text||'*COALESCE(flight.schedule.CRS_DEP_HOUR, tmp_imputed_CRS_DEP_HOUR.CRS_DEP_HOUR)'||_params[4]::text||'*CRS_DEP_MIN+('||_params[5]::text||'*CRS_ARR_HOUR)+('||_params[6]::text||'*CRS_ARR_MIN)) as val FROM tmp_distances_DISTANCE JOIN flight.schedule ON tmp_distances_DISTANCE.airports = flight.schedule.airports LEFT OUTER JOIN tmp_imputed_CRS_DEP_HOUR ON tmp_imputed_CRS_DEP_HOUR.flight = flight.schedule.flight GROUP BY flight.schedule.flight, flight.schedule.airports) as schedule
JOIN
(SELECT flight.flights.flight, SUM('||_params[7]::text||'*COALESCE(flight.flights.TAXI_OUT, tmp_imputed_TAXI_OUT.TAXI_OUT)+('||_params[8]::text||'*COALESCE(flight.flights.TAXI_IN, tmp_imputed_TAXI_IN.TAXI_IN))+
('||_params[9]::text||'*COALESCE(flight.flights.DIVERTED, tmp_imputed_DIVERTED.DIVERTED))+('||_params[10]::text||'*COALESCE(flight.flights.ARR_DELAY, tmp_imputed_ARR_DELAY.ARR_DELAY))+('||_params[11]::text||'*COALESCE(flight.flights.DEP_DELAY, tmp_imputed_DEP_DELAY.DEP_DELAY))+
'||_params[12]::text||'*ACTUAL_ELAPSED_TIME + '||_params[13]::text||'*AIR_TIME + '||_params[14]::text||'*DEP_TIME_HOUR + '||_params[15]::text||'*DEP_TIME_MIN + '||_params[16]::text||'*WHEELS_OFF_HOUR + '||_params[17]::text||'*WHEELS_OFF_MIN + '||_params[18]::text||'*WHEELS_ON_HOUR + '||_params[19]::text||'*WHEELS_ON_MIN + '||_params[20]::text||'*ARR_TIME_HOUR + '||_params[21]::text||'*ARR_TIME_MIN + '||_params[22]::text||'*MONTH_SIN + '||_params[23]::text||'*MONTH_COS + '||_params[24]::text||'*DAY_SIN + '||_params[25]::text||'*DAY_COS + '||_params[26]::text||'*WEEKDAY_SIN + '||_params[27]::text||'*WEEKDAY_COS + '||_params[28]::text||'*EXTRA_DAY_ARR + '||_params[29]::text||'*EXTRA_DAY_DEP) as val
FROM flight.flights LEFT OUTER JOIN tmp_imputed_TAXI_OUT ON flight.flights.id = tmp_imputed_TAXI_OUT.id
LEFT OUTER JOIN tmp_imputed_TAXI_IN ON flight.flights.id = tmp_imputed_TAXI_IN.id
LEFT OUTER JOIN tmp_imputed_DIVERTED ON flight.flights.id = tmp_imputed_DIVERTED.id
LEFT OUTER JOIN tmp_imputed_ARR_DELAY ON flight.flights.id = tmp_imputed_ARR_DELAY.id
LEFT OUTER JOIN tmp_imputed_DEP_DELAY ON flight.flights.id = tmp_imputed_DEP_DELAY.id GROUP BY flight.flights.flight) AS flights
ON schedule.flight = flights.flight GROUP BY schedule.airports)';

                query := 'INSERT INTO tmp_imputed_DISTANCE
(SELECT tmp_distances_DISTANCE.airports, AVG((('||_params[1]::text || ')+' || _params[3]::text||'*COALESCE(flight.schedule.CRS_DEP_HOUR, tmp_imputed_CRS_DEP_HOUR.CRS_DEP_HOUR))+
('||_params[4]::text||'*CRS_DEP_MIN)+('||_params[5]::text||'*CRS_ARR_HOUR)+('||_params[6]::text||'*CRS_ARR_MIN)+('||_params[7]::text||'*COALESCE(flight.flights.TAXI_OUT, tmp_imputed_TAXI_OUT.TAXI_OUT))+('||_params[8]::text||'*COALESCE(flight.flights.TAXI_IN, tmp_imputed_TAXI_IN.TAXI_IN))+
('||_params[9]::text||'*COALESCE(flight.flights.DIVERTED, tmp_imputed_DIVERTED.DIVERTED))+('||_params[10]::text||'*COALESCE(flight.flights.ARR_DELAY, tmp_imputed_ARR_DELAY.ARR_DELAY))+('||_params[11]::text||'*COALESCE(flight.flights.DEP_DELAY, tmp_imputed_DEP_DELAY.DEP_DELAY))+
'||_params[12]::text||'*ACTUAL_ELAPSED_TIME + '||_params[13]::text||'*AIR_TIME + '||_params[14]::text||'*DEP_TIME_HOUR + '||_params[15]::text||'*DEP_TIME_MIN + '||_params[16]::text||'*WHEELS_OFF_HOUR + '||_params[17]::text||'*WHEELS_OFF_MIN + '||_params[18]::text||'*WHEELS_ON_HOUR + '||_params[19]::text||'*WHEELS_ON_MIN + '||_params[20]::text||'*ARR_TIME_HOUR + '||_params[21]::text||'*ARR_TIME_MIN + '||_params[22]::text||'*MONTH_SIN + '||_params[23]::text||'*MONTH_COS + '||_params[24]::text||'*DAY_SIN + '||_params[25]::text||'*DAY_COS + '||_params[26]::text||'*WEEKDAY_SIN + '||_params[27]::text||'*WEEKDAY_COS + '||_params[28]::text||'*EXTRA_DAY_ARR + '||_params[29]::text||'*EXTRA_DAY_DEP)
FROM tmp_distances_DISTANCE JOIN flight.schedule ON tmp_distances_DISTANCE.airports = flight.schedule.airports
JOIN flight.flights ON flight.schedule.flight = flight.flights.flight
LEFT OUTER JOIN tmp_imputed_CRS_DEP_HOUR ON flight.schedule.flight = tmp_imputed_CRS_DEP_HOUR.flight
LEFT OUTER JOIN tmp_imputed_TAXI_OUT ON flight.flights.id = tmp_imputed_TAXI_OUT.id
LEFT OUTER JOIN tmp_imputed_TAXI_IN ON flight.flights.id = tmp_imputed_TAXI_IN.id
LEFT OUTER JOIN tmp_imputed_DIVERTED ON flight.flights.id = tmp_imputed_DIVERTED.id
LEFT OUTER JOIN tmp_imputed_ARR_DELAY ON flight.flights.id = tmp_imputed_ARR_DELAY.id
LEFT OUTER JOIN tmp_imputed_DEP_DELAY ON flight.flights.id = tmp_imputed_DEP_DELAY.id GROUP BY tmp_distances_DISTANCE.airports)';

                RAISE NOTICE 'Query: % ' , query;
                EXECUTE query;

                EXECUTE query_cof_diff INTO _triple_removal;
                _triple := _triple_train + _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Impute new values and update cofactor matrix: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));

                ---CRS_DEP_HOUR
                RAISE NOTICE '--------------IMPUTING CRS DEP HOUR---------------------';
                query_cof_diff := 'SELECT SUM(join_1.cof_3 * flights.cof_3) FROM
(SELECT flight, SUM(distances.cof_1*schedule.cof_2) AS cof_3 FROM
(SELECT tmp_schedule_CRS_DEP_HOUR.flight, tmp_schedule_CRS_DEP_HOUR.airports,
SUM(to_cofactor(ARRAY[tmp_imputed_CRS_DEP_HOUR.CRS_DEP_HOUR, (CRS_DEP_MIN), (CRS_ARR_HOUR), (CRS_ARR_MIN)], ARRAY[]::int[])) as cof_2
FROM
tmp_schedule_CRS_DEP_HOUR JOIN tmp_imputed_CRS_DEP_HOUR ON tmp_schedule_CRS_DEP_HOUR.flight = tmp_imputed_CRS_DEP_HOUR.flight GROUP BY tmp_schedule_CRS_DEP_HOUR.flight, tmp_schedule_CRS_DEP_HOUR.airports) as schedule
JOIN
(SELECT flight.distances.airports, SUM(cont_to_cofactor(COALESCE(flight.distances.distance, tmp_imputed_distance.distance))) as cof_1
FROM flight.distances JOIN tmp_imputed_distance ON flight.distances.airports = tmp_imputed_distance.airports GROUP BY flight.distances.airports) as distances
ON distances.airports = schedule.airports GROUP BY schedule.flight
) as join_1
JOIN
(
SELECT flight.flights.flight, SUM(to_cofactor(ARRAY[COALESCE(flight.flights.TAXI_OUT, tmp_imputed_TAXI_OUT.TAXI_OUT), (COALESCE(flight.flights.TAXI_IN, tmp_imputed_TAXI_IN.TAXI_IN)), (COALESCE(flight.flights.DIVERTED, tmp_imputed_DIVERTED.DIVERTED)), (COALESCE(flight.flights.ARR_DELAY, tmp_imputed_ARR_DELAY.ARR_DELAY)), (COALESCE(flight.flights.DEP_DELAY, tmp_imputed_DEP_DELAY.DEP_DELAY)), (ACTUAL_ELAPSED_TIME), (AIR_TIME), (DEP_TIME_HOUR), (DEP_TIME_MIN), (WHEELS_OFF_HOUR), (WHEELS_OFF_MIN), (WHEELS_ON_HOUR), (WHEELS_ON_MIN), (ARR_TIME_HOUR), (ARR_TIME_MIN), (MONTH_SIN), (MONTH_COS), (DAY_SIN), (DAY_COS), (WEEKDAY_SIN), (WEEKDAY_COS), (EXTRA_DAY_ARR), (EXTRA_DAY_DEP)], ARRAY[]::int[])) AS cof_3 FROM flight.flights
LEFT OUTER JOIN tmp_imputed_TAXI_OUT ON flight.flights.id = tmp_imputed_TAXI_OUT.id
LEFT OUTER JOIN tmp_imputed_TAXI_IN ON flight.flights.id = tmp_imputed_TAXI_IN.id
LEFT OUTER JOIN tmp_imputed_DIVERTED ON flight.flights.id = tmp_imputed_DIVERTED.id
LEFT OUTER JOIN tmp_imputed_ARR_DELAY ON flight.flights.id = tmp_imputed_ARR_DELAY.id
LEFT OUTER JOIN tmp_imputed_DEP_DELAY ON flight.flights.id = tmp_imputed_DEP_DELAY.id
 GROUP BY flight.flights.flight
) as flights ON join_1.flight = flights.flight';
                _start_ts := clock_timestamp();
                execute(query_cof_diff) INTO _triple_removal;
                _triple_train := _triple - _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE ' SELECT: time: %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _label_idx := 1;
                _params := ridge_linear_regression(_triple_train, _label_idx, 0.001, 1, 1000);
                EXECUTE 'TRUNCATE TABLE tmp_imputed_CRS_DEP_HOUR';
                RAISE NOTICE ' TRUNCATE DONE ';

                query := 'INSERT INTO tmp_imputed_CRS_DEP_HOUR (SELECT join_1.flight, SUM(join_1.val + flights.val) FROM
(SELECT schedule.flight, ('||_params[1]::text || ')+ SUM(distances.val+schedule.val) as val FROM
(SELECT flight.distances.airports, SUM(' ||_params[2]::text||' *COALESCE(flight.distances.DISTANCE, tmp_imputed_DISTANCE.DISTANCE)) as val FROM flight.distances LEFT OUTER JOIN tmp_imputed_DISTANCE ON flight.distances.airports = tmp_imputed_DISTANCE.airports GROUP BY flight.distances.airports) as distances
JOIN
(SELECT tmp_schedule_CRS_DEP_HOUR.flight, tmp_schedule_CRS_DEP_HOUR.airports, SUM('||_params[4]::text||'*CRS_DEP_MIN+('||_params[5]::text||'*CRS_ARR_HOUR)+('||_params[6]::text||'*CRS_ARR_MIN)) as val FROM
tmp_schedule_CRS_DEP_HOUR GROUP BY tmp_schedule_CRS_DEP_HOUR.flight, tmp_schedule_CRS_DEP_HOUR.airports) as schedule
ON distances.airports = schedule.airports GROUP BY schedule.flight) as join_1
JOIN
(SELECT flight.flights.flight, SUM('||_params[7]::text||'*COALESCE(flight.flights.TAXI_OUT, tmp_imputed_TAXI_OUT.TAXI_OUT)+('||_params[8]::text||'*COALESCE(flight.flights.TAXI_IN, tmp_imputed_TAXI_IN.TAXI_IN))+
('||_params[9]::text||'*COALESCE(flight.flights.DIVERTED, tmp_imputed_DIVERTED.DIVERTED))+('||_params[10]::text||'*COALESCE(flight.flights.ARR_DELAY, tmp_imputed_ARR_DELAY.ARR_DELAY))+('||_params[11]::text||'*COALESCE(flight.flights.DEP_DELAY, tmp_imputed_DEP_DELAY.DEP_DELAY))+
'||_params[12]::text||'*ACTUAL_ELAPSED_TIME + '||_params[13]::text||'*AIR_TIME + '||_params[14]::text||'*DEP_TIME_HOUR + '||_params[15]::text||'*DEP_TIME_MIN + '||_params[16]::text||'*WHEELS_OFF_HOUR + '||_params[17]::text||'*WHEELS_OFF_MIN + '||_params[18]::text||'*WHEELS_ON_HOUR + '||_params[19]::text||'*WHEELS_ON_MIN + '||_params[20]::text||'*ARR_TIME_HOUR + '||_params[21]::text||'*ARR_TIME_MIN + '||_params[22]::text||'*MONTH_SIN + '||_params[23]::text||'*MONTH_COS + '||_params[24]::text||'*DAY_SIN + '||_params[25]::text||'*DAY_COS + '||_params[26]::text||'*WEEKDAY_SIN + '||_params[27]::text||'*WEEKDAY_COS + '||_params[28]::text||'*EXTRA_DAY_ARR + '||_params[29]::text||'*EXTRA_DAY_DEP) as val
FROM flight.flights LEFT OUTER JOIN tmp_imputed_TAXI_OUT ON flight.flights.id = tmp_imputed_TAXI_OUT.id
LEFT OUTER JOIN tmp_imputed_TAXI_IN ON flight.flights.id = tmp_imputed_TAXI_IN.id
LEFT OUTER JOIN tmp_imputed_DIVERTED ON flight.flights.id = tmp_imputed_DIVERTED.id
LEFT OUTER JOIN tmp_imputed_ARR_DELAY ON flight.flights.id = tmp_imputed_ARR_DELAY.id
LEFT OUTER JOIN tmp_imputed_DEP_DELAY ON flight.flights.id = tmp_imputed_DEP_DELAY.id GROUP BY flight.flights.flight) AS flights
ON join_1.flight = flights.flight GROUP BY join_1.flight)';

                query := 'INSERT INTO tmp_imputed_CRS_DEP_HOUR
(SELECT tmp_schedule_CRS_DEP_HOUR.flight, AVG(('||_params[1]::text || ')+(' ||_params[2]::text||'*COALESCE(flight.distances.DISTANCE, tmp_imputed_DISTANCE.DISTANCE))+
('||_params[4]::text||'*CRS_DEP_MIN)+('||_params[5]::text||'*CRS_ARR_HOUR)+('||_params[6]::text||'*CRS_ARR_MIN)+('||_params[7]::text||'*COALESCE(flight.flights.TAXI_OUT, tmp_imputed_TAXI_OUT.TAXI_OUT))+('||_params[8]::text||'*COALESCE(flight.flights.TAXI_IN, tmp_imputed_TAXI_IN.TAXI_IN))+
('||_params[9]::text||'*COALESCE(flight.flights.DIVERTED, tmp_imputed_DIVERTED.DIVERTED))+('||_params[10]::text||'*COALESCE(flight.flights.ARR_DELAY, tmp_imputed_ARR_DELAY.ARR_DELAY))+('||_params[11]::text||'*COALESCE(flight.flights.DEP_DELAY, tmp_imputed_DEP_DELAY.DEP_DELAY))+
'||_params[12]::text||'*ACTUAL_ELAPSED_TIME + '||_params[13]::text||'*AIR_TIME + '||_params[14]::text||'*DEP_TIME_HOUR + '||_params[15]::text||'*DEP_TIME_MIN + '||_params[16]::text||'*WHEELS_OFF_HOUR + '||_params[17]::text||'*WHEELS_OFF_MIN + '||_params[18]::text||'*WHEELS_ON_HOUR + '||_params[19]::text||'*WHEELS_ON_MIN + '||_params[20]::text||'*ARR_TIME_HOUR + '||_params[21]::text||'*ARR_TIME_MIN + '||_params[22]::text||'*MONTH_SIN + '||_params[23]::text||'*MONTH_COS + '||_params[24]::text||'*DAY_SIN + '||_params[25]::text||'*DAY_COS + '||_params[26]::text||'*WEEKDAY_SIN + '||_params[27]::text||'*WEEKDAY_COS + '||_params[28]::text||'*EXTRA_DAY_ARR + '||_params[29]::text||'*EXTRA_DAY_DEP)
FROM tmp_schedule_CRS_DEP_HOUR JOIN flight.distances ON flight.distances.airports = tmp_schedule_CRS_DEP_HOUR.airports
JOIN flight.flights ON tmp_schedule_CRS_DEP_HOUR.flight = flight.flights.flight
LEFT OUTER JOIN tmp_imputed_DISTANCE ON flight.distances.airports = tmp_imputed_DISTANCE.airports
LEFT OUTER JOIN tmp_imputed_TAXI_OUT ON flight.flights.id = tmp_imputed_TAXI_OUT.id
LEFT OUTER JOIN tmp_imputed_TAXI_IN ON flight.flights.id = tmp_imputed_TAXI_IN.id
LEFT OUTER JOIN tmp_imputed_DIVERTED ON flight.flights.id = tmp_imputed_DIVERTED.id
LEFT OUTER JOIN tmp_imputed_ARR_DELAY ON flight.flights.id = tmp_imputed_ARR_DELAY.id
LEFT OUTER JOIN tmp_imputed_DEP_DELAY ON flight.flights.id = tmp_imputed_DEP_DELAY.id GROUP BY tmp_schedule_CRS_DEP_HOUR.flight)';

                RAISE NOTICE ' % ', query;

                _start_ts := clock_timestamp();
                EXECUTE query;
                _end_ts   := clock_timestamp();
                RAISE NOTICE ' INSERT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _start_ts := clock_timestamp();
                EXECUTE query_cof_diff INTO _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE ' SELECT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _triple := _triple_train + _triple_removal;
                ---taxi out
                RAISE NOTICE '--------------TAXI OUT---------------------';
                query_cof_diff := 'SELECT SUM(join_1.cof_3 * flights.cof_3) FROM
(SELECT tmp_flights_TAXI_OUT.flight, SUM(to_cofactor(ARRAY[(tmp_imputed_TAXI_OUT.TAXI_OUT), (COALESCE(tmp_flights_TAXI_OUT.TAXI_IN, tmp_imputed_TAXI_IN.TAXI_IN)), (COALESCE(tmp_flights_TAXI_OUT.DIVERTED, tmp_imputed_DIVERTED.DIVERTED)), (COALESCE(tmp_flights_TAXI_OUT.ARR_DELAY, tmp_imputed_ARR_DELAY.ARR_DELAY)), (COALESCE(tmp_flights_TAXI_OUT.DEP_DELAY, tmp_imputed_DEP_DELAY.DEP_DELAY)), (ACTUAL_ELAPSED_TIME), (AIR_TIME), (DEP_TIME_HOUR), (DEP_TIME_MIN), (WHEELS_OFF_HOUR), (WHEELS_OFF_MIN), (WHEELS_ON_HOUR), (WHEELS_ON_MIN), (ARR_TIME_HOUR), (ARR_TIME_MIN), (MONTH_SIN), (MONTH_COS), (DAY_SIN), (DAY_COS), (WEEKDAY_SIN), (WEEKDAY_COS), (EXTRA_DAY_ARR), (EXTRA_DAY_DEP)], ARRAY[]::int[])) AS cof_3 FROM
tmp_flights_TAXI_OUT JOIN tmp_imputed_TAXI_OUT ON tmp_flights_TAXI_OUT.id = tmp_imputed_TAXI_OUT.id
LEFT OUTER JOIN tmp_imputed_TAXI_IN ON tmp_flights_TAXI_OUT.id = tmp_imputed_TAXI_IN.id
LEFT OUTER JOIN tmp_imputed_DIVERTED ON tmp_flights_TAXI_OUT.id = tmp_imputed_DIVERTED.id
LEFT OUTER JOIN tmp_imputed_ARR_DELAY ON tmp_flights_TAXI_OUT.id = tmp_imputed_ARR_DELAY.id
LEFT OUTER JOIN tmp_imputed_DEP_DELAY ON tmp_flights_TAXI_OUT.id = tmp_imputed_DEP_DELAY.id
 GROUP BY tmp_flights_TAXI_OUT.flight
) as flights JOIN
(SELECT flight, SUM(distances.cof_1*schedule.cof_2) AS cof_3 FROM
(SELECT flight.schedule.flight, flight.schedule.airports,
SUM(to_cofactor(ARRAY[tmp_imputed_CRS_DEP_HOUR.CRS_DEP_HOUR, (CRS_DEP_MIN), (CRS_ARR_HOUR), (CRS_ARR_MIN)], ARRAY[]::int[])) as cof_2
FROM
flight.schedule LEFT OUTER JOIN tmp_imputed_CRS_DEP_HOUR ON flight.schedule.flight = tmp_imputed_CRS_DEP_HOUR.flight GROUP BY flight.schedule.flight, flight.schedule.airports) as schedule
JOIN
(SELECT flight.distances.airports, SUM(cont_to_cofactor(COALESCE(flight.distances.distance, tmp_imputed_distance.distance))) as cof_1
FROM flight.distances JOIN tmp_imputed_distance ON flight.distances.airports = tmp_imputed_distance.airports GROUP BY flight.distances.airports) as distances
ON distances.airports = schedule.airports GROUP BY schedule.flight
) as join_1
ON join_1.flight = flights.flight';
                _start_ts := clock_timestamp();
                execute(query_cof_diff) INTO _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE ' SELECT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _triple_train := _triple - _triple_removal;

                _label_idx := 5;
                _params := ridge_linear_regression(_triple_train, _label_idx, 0.001, 1, 1000);

                EXECUTE 'TRUNCATE TABLE tmp_imputed_TAXI_OUT';
                RAISE NOTICE ' TRUNCATE DONE ';

                query := 'INSERT INTO tmp_imputed_TAXI_OUT (SELECT flights.id, SUM(join_1.val + flights.val) FROM
(SELECT tmp_flights_TAXI_OUT.flight, tmp_flights_TAXI_OUT.id, SUM(('||_params[8]::text||'*COALESCE(tmp_flights_TAXI_OUT.TAXI_IN, tmp_imputed_TAXI_IN.TAXI_IN))+
('||_params[9]::text||'*COALESCE(tmp_flights_TAXI_OUT.DIVERTED, tmp_imputed_DIVERTED.DIVERTED))+('||_params[10]::text||'*COALESCE(tmp_flights_TAXI_OUT.ARR_DELAY, tmp_imputed_ARR_DELAY.ARR_DELAY))+('||_params[11]::text||'*COALESCE(tmp_flights_TAXI_OUT.DEP_DELAY, tmp_imputed_DEP_DELAY.DEP_DELAY))+
'||_params[12]::text||'*ACTUAL_ELAPSED_TIME + '||_params[13]::text||'*AIR_TIME + '||_params[14]::text||'*DEP_TIME_HOUR + '||_params[15]::text||'*DEP_TIME_MIN + '||_params[16]::text||'*WHEELS_OFF_HOUR + '||_params[17]::text||'*WHEELS_OFF_MIN + '||_params[18]::text||'*WHEELS_ON_HOUR + '||_params[19]::text||'*WHEELS_ON_MIN + '||_params[20]::text||'*ARR_TIME_HOUR + '||_params[21]::text||'*ARR_TIME_MIN + '||_params[22]::text||'*MONTH_SIN + '||_params[23]::text||'*MONTH_COS + '||_params[24]::text||'*DAY_SIN + '||_params[25]::text||'*DAY_COS + '||_params[26]::text||'*WEEKDAY_SIN + '||_params[27]::text||'*WEEKDAY_COS + '||_params[28]::text||'*EXTRA_DAY_ARR + '||_params[29]::text||'*EXTRA_DAY_DEP) as val
FROM tmp_flights_TAXI_OUT LEFT OUTER JOIN tmp_imputed_TAXI_IN ON tmp_flights_TAXI_OUT.id = tmp_imputed_TAXI_IN.id
LEFT OUTER JOIN tmp_imputed_DIVERTED ON tmp_flights_TAXI_OUT.id = tmp_imputed_DIVERTED.id
LEFT OUTER JOIN tmp_imputed_ARR_DELAY ON tmp_flights_TAXI_OUT.id = tmp_imputed_ARR_DELAY.id
LEFT OUTER JOIN tmp_imputed_DEP_DELAY ON tmp_flights_TAXI_OUT.id = tmp_imputed_DEP_DELAY.id GROUP BY tmp_flights_TAXI_OUT.flight, tmp_flights_TAXI_OUT.id) AS flights
JOIN
(SELECT schedule.flight, ('||_params[1]::text || ')+ SUM(distances.val+schedule.val) as val FROM
(SELECT flight.distances.airports, SUM(' ||_params[2]::text||' *COALESCE(flight.distances.DISTANCE, tmp_imputed_DISTANCE.DISTANCE)) as val FROM flight.distances LEFT OUTER JOIN tmp_imputed_DISTANCE ON flight.distances.airports = tmp_imputed_DISTANCE.airports GROUP BY flight.distances.airports) as distances
JOIN
(SELECT flight.schedule.flight, flight.schedule.airports, SUM('||_params[3]::text||'*COALESCE(flight.schedule.CRS_DEP_HOUR, tmp_imputed_CRS_DEP_HOUR.CRS_DEP_HOUR)'||_params[4]::text||'*CRS_DEP_MIN+('||_params[5]::text||'*CRS_ARR_HOUR)+('||_params[6]::text||'*CRS_ARR_MIN)) as val FROM
flight.schedule LEFT OUTER JOIN tmp_imputed_CRS_DEP_HOUR ON flight.schedule.flight = tmp_imputed_CRS_DEP_HOUR.flight GROUP BY flight.schedule.flight, flight.schedule.airports) as schedule
ON distances.airports = schedule.airports GROUP BY schedule.flight) as join_1
ON join_1.flight = flights.flight GROUP BY flights.id)';

                query := 'INSERT INTO tmp_imputed_TAXI_OUT
(SELECT tmp_flights_TAXI_OUT.id, ('||_params[1]::text || ')+(' ||_params[2]::text||'*COALESCE(flight.distances.DISTANCE, tmp_imputed_DISTANCE.DISTANCE))+('||_params[3]::text||'*COALESCE(flight.schedule.CRS_DEP_HOUR, tmp_imputed_CRS_DEP_HOUR.CRS_DEP_HOUR))+
('||_params[4]::text||'*CRS_DEP_MIN)+('||_params[5]::text||'*CRS_ARR_HOUR)+('||_params[6]::text||'*CRS_ARR_MIN)+('||_params[8]::text||'*COALESCE(tmp_flights_TAXI_OUT.TAXI_IN, tmp_imputed_TAXI_IN.TAXI_IN))+
('||_params[9]::text||'*COALESCE(tmp_flights_TAXI_OUT.DIVERTED, tmp_imputed_DIVERTED.DIVERTED))+('||_params[10]::text||'*COALESCE(tmp_flights_TAXI_OUT.ARR_DELAY, tmp_imputed_ARR_DELAY.ARR_DELAY))+('||_params[11]::text||'*COALESCE(tmp_flights_TAXI_OUT.DEP_DELAY, tmp_imputed_DEP_DELAY.DEP_DELAY))+
'||_params[12]::text||'*ACTUAL_ELAPSED_TIME + '||_params[13]::text||'*AIR_TIME + '||_params[14]::text||'*DEP_TIME_HOUR + '||_params[15]::text||'*DEP_TIME_MIN + '||_params[16]::text||'*WHEELS_OFF_HOUR + '||_params[17]::text||'*WHEELS_OFF_MIN + '||_params[18]::text||'*WHEELS_ON_HOUR + '||_params[19]::text||'*WHEELS_ON_MIN + '||_params[20]::text||'*ARR_TIME_HOUR + '||_params[21]::text||'*ARR_TIME_MIN + '||_params[22]::text||'*MONTH_SIN + '||_params[23]::text||'*MONTH_COS + '||_params[24]::text||'*DAY_SIN + '||_params[25]::text||'*DAY_COS + '||_params[26]::text||'*WEEKDAY_SIN + '||_params[27]::text||'*WEEKDAY_COS + '||_params[28]::text||'*EXTRA_DAY_ARR + '||_params[29]::text||'*EXTRA_DAY_DEP
FROM tmp_flights_TAXI_OUT JOIN flight.schedule ON tmp_flights_TAXI_OUT.flight = flight.schedule.flight
JOIN flight.distances ON flight.distances.airports = flight.schedule.airports
LEFT OUTER JOIN tmp_imputed_CRS_DEP_HOUR ON flight.schedule.flight = tmp_imputed_CRS_DEP_HOUR.flight
LEFT OUTER JOIN tmp_imputed_DISTANCE ON flight.distances.airports = tmp_imputed_DISTANCE.airports
LEFT OUTER JOIN tmp_imputed_TAXI_IN ON tmp_flights_TAXI_OUT.id = tmp_imputed_TAXI_IN.id
LEFT OUTER JOIN tmp_imputed_DIVERTED ON tmp_flights_TAXI_OUT.id = tmp_imputed_DIVERTED.id
LEFT OUTER JOIN tmp_imputed_ARR_DELAY ON tmp_flights_TAXI_OUT.id = tmp_imputed_ARR_DELAY.id
LEFT OUTER JOIN tmp_imputed_DEP_DELAY ON tmp_flights_TAXI_OUT.id = tmp_imputed_DEP_DELAY.id)';

                _start_ts := clock_timestamp();
                EXECUTE query;
                _end_ts   := clock_timestamp();

                RAISE NOTICE ' INSERT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                                _start_ts := clock_timestamp();

                EXECUTE query_cof_diff INTO _triple_removal;
                                _end_ts   := clock_timestamp();

                RAISE NOTICE ' % ', _triple_removal;
                                _end_ts   := clock_timestamp();
                RAISE NOTICE ' SELECT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _triple := _triple_train + _triple_removal;

            ------TAXI IN
            RAISE NOTICE '--------------TAXI IN---------------------';
            query_cof_diff := 'SELECT SUM(join_1.cof_3 * flights.cof_3) FROM
(SELECT tmp_flights_TAXI_IN.flight, SUM(to_cofactor(ARRAY[(COALESCE(tmp_flights_TAXI_IN.TAXI_OUT, tmp_imputed_TAXI_OUT.TAXI_OUT)),(tmp_imputed_TAXI_IN.TAXI_IN), (COALESCE(tmp_flights_TAXI_IN.DIVERTED, tmp_imputed_DIVERTED.DIVERTED)), (COALESCE(tmp_flights_TAXI_IN.ARR_DELAY, tmp_imputed_ARR_DELAY.ARR_DELAY)), (COALESCE(tmp_flights_TAXI_IN.DEP_DELAY, tmp_imputed_DEP_DELAY.DEP_DELAY)), (ACTUAL_ELAPSED_TIME), (AIR_TIME), (DEP_TIME_HOUR), (DEP_TIME_MIN), (WHEELS_OFF_HOUR), (WHEELS_OFF_MIN), (WHEELS_ON_HOUR), (WHEELS_ON_MIN), (ARR_TIME_HOUR), (ARR_TIME_MIN), (MONTH_SIN), (MONTH_COS), (DAY_SIN), (DAY_COS), (WEEKDAY_SIN), (WEEKDAY_COS), (EXTRA_DAY_ARR), (EXTRA_DAY_DEP)], ARRAY[]::int[])) AS cof_3 FROM
tmp_flights_TAXI_IN JOIN tmp_imputed_TAXI_IN ON tmp_flights_TAXI_IN.id = tmp_imputed_TAXI_IN.id
LEFT OUTER JOIN tmp_imputed_TAXI_OUT ON tmp_flights_TAXI_IN.id = tmp_imputed_TAXI_OUT.id
LEFT OUTER JOIN tmp_imputed_DIVERTED ON tmp_flights_TAXI_IN.id = tmp_imputed_DIVERTED.id
LEFT OUTER JOIN tmp_imputed_ARR_DELAY ON tmp_flights_TAXI_IN.id = tmp_imputed_ARR_DELAY.id
LEFT OUTER JOIN tmp_imputed_DEP_DELAY ON tmp_flights_TAXI_IN.id = tmp_imputed_DEP_DELAY.id
 GROUP BY tmp_flights_TAXI_IN.flight
) as flights JOIN
(SELECT flight, SUM(distances.cof_1*schedule.cof_2) AS cof_3 FROM
(SELECT flight.schedule.flight, flight.schedule.airports,
SUM(to_cofactor(ARRAY[tmp_imputed_CRS_DEP_HOUR.CRS_DEP_HOUR, (CRS_DEP_MIN), (CRS_ARR_HOUR), (CRS_ARR_MIN)], ARRAY[]::int[])) as cof_2
FROM
flight.schedule LEFT OUTER JOIN tmp_imputed_CRS_DEP_HOUR ON flight.schedule.flight = tmp_imputed_CRS_DEP_HOUR.flight GROUP BY flight.schedule.flight, flight.schedule.airports) as schedule
JOIN
(SELECT flight.distances.airports, SUM(cont_to_cofactor(COALESCE(flight.distances.distance, tmp_imputed_distance.distance))) as cof_1
FROM flight.distances JOIN tmp_imputed_distance ON flight.distances.airports = tmp_imputed_distance.airports GROUP BY flight.distances.airports) as distances
ON distances.airports = schedule.airports GROUP BY schedule.flight
) as join_1
ON join_1.flight = flights.flight';
                _start_ts := clock_timestamp();
                execute(query_cof_diff) INTO _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE ' SELECT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _triple_train := _triple - _triple_removal;

                _label_idx := 6;
                _params := ridge_linear_regression(_triple_train, _label_idx, 0.001, 1, 1000);
                RAISE NOTICE 'params %', _params;
                EXECUTE 'TRUNCATE TABLE tmp_imputed_TAXI_IN';
                RAISE NOTICE ' TRUNCATE DONE ';

                query := 'INSERT INTO tmp_imputed_TAXI_IN (SELECT flights.id, SUM(join_1.val + flights.val) FROM
(SELECT tmp_flights_TAXI_IN.flight, tmp_flights_TAXI_IN.id, SUM(('||_params[7]::text||'*COALESCE(tmp_flights_TAXI_IN.TAXI_OUT, tmp_imputed_TAXI_OUT.TAXI_OUT))+
('||_params[9]::text||'*COALESCE(tmp_flights_TAXI_IN.DIVERTED, tmp_imputed_DIVERTED.DIVERTED))+('||_params[10]::text||'*COALESCE(tmp_flights_TAXI_IN.ARR_DELAY, tmp_imputed_ARR_DELAY.ARR_DELAY))+('||_params[11]::text||'*COALESCE(tmp_flights_TAXI_IN.DEP_DELAY, tmp_imputed_DEP_DELAY.DEP_DELAY))+
'||_params[12]::text||'*ACTUAL_ELAPSED_TIME + '||_params[13]::text||'*AIR_TIME + '||_params[14]::text||'*DEP_TIME_HOUR + '||_params[15]::text||'*DEP_TIME_MIN + '||_params[16]::text||'*WHEELS_OFF_HOUR + '||_params[17]::text||'*WHEELS_OFF_MIN + '||_params[18]::text||'*WHEELS_ON_HOUR + '||_params[19]::text||'*WHEELS_ON_MIN + '||_params[20]::text||'*ARR_TIME_HOUR + '||_params[21]::text||'*ARR_TIME_MIN + '||_params[22]::text||'*MONTH_SIN + '||_params[23]::text||'*MONTH_COS + '||_params[24]::text||'*DAY_SIN + '||_params[25]::text||'*DAY_COS + '||_params[26]::text||'*WEEKDAY_SIN + '||_params[27]::text||'*WEEKDAY_COS + '||_params[28]::text||'*EXTRA_DAY_ARR + '||_params[29]::text||'*EXTRA_DAY_DEP) as val
FROM tmp_flights_TAXI_IN LEFT OUTER JOIN tmp_imputed_TAXI_OUT ON flight.flights.id = tmp_imputed_TAXI_OUT.id
LEFT OUTER JOIN tmp_imputed_DIVERTED ON tmp_flights_TAXI_IN.id = tmp_imputed_DIVERTED.id
LEFT OUTER JOIN tmp_imputed_ARR_DELAY ON tmp_flights_TAXI_IN.id = tmp_imputed_ARR_DELAY.id
LEFT OUTER JOIN tmp_imputed_DEP_DELAY ON tmp_flights_TAXI_IN.id = tmp_imputed_DEP_DELAY.id GROUP BY tmp_flights_TAXI_IN.flight, tmp_flights_TAXI_IN.id) AS flights
JOIN
(SELECT schedule.flight, ('||_params[1]::text || ')+ SUM(distances.val+schedule.val) as val FROM
(SELECT flight.distances.airports, SUM(' ||_params[2]::text||' *COALESCE(flight.distances.DISTANCE, tmp_imputed_DISTANCE.DISTANCE)) as val FROM flight.distances LEFT OUTER JOIN tmp_imputed_DISTANCE ON flight.distances.airports = tmp_imputed_DISTANCE.airports GROUP BY flight.distances.airports) as distances
JOIN
(SELECT flight.schedule.flight, flight.schedule.airports, SUM('||_params[3]::text||'*COALESCE(flight.schedule.CRS_DEP_HOUR, tmp_imputed_CRS_DEP_HOUR.CRS_DEP_HOUR)'||_params[4]::text||'*CRS_DEP_MIN+('||_params[5]::text||'*CRS_ARR_HOUR)+('||_params[6]::text||'*CRS_ARR_MIN)) as val FROM
flight.schedule LEFT OUTER JOIN tmp_imputed_CRS_DEP_HOUR ON flight.schedule.flight = tmp_imputed_CRS_DEP_HOUR.flight GROUP BY flight.schedule.flight, flight.schedule.airports) as schedule
ON distances.airports = schedule.airports GROUP BY schedule.flight) as join_1
ON join_1.flight = flights.flight GROUP BY flights.id)';

                query := 'INSERT INTO tmp_imputed_TAXI_IN
(SELECT tmp_flights_TAXI_IN.id, ('||_params[1]::text || ')+(' ||_params[2]::text||'*COALESCE(flight.distances.DISTANCE, tmp_imputed_DISTANCE.DISTANCE))+('||_params[3]::text||'*COALESCE(flight.schedule.CRS_DEP_HOUR, tmp_imputed_CRS_DEP_HOUR.CRS_DEP_HOUR))+
('||_params[4]::text||'*CRS_DEP_MIN)+('||_params[5]::text||'*CRS_ARR_HOUR)+('||_params[6]::text||'*CRS_ARR_MIN)+('||_params[7]::text||'*COALESCE(tmp_flights_TAXI_IN.TAXI_OUT, tmp_imputed_TAXI_OUT.TAXI_OUT))+
('||_params[9]::text||'*COALESCE(tmp_flights_TAXI_IN.DIVERTED, tmp_imputed_DIVERTED.DIVERTED))+('||_params[10]::text||'*COALESCE(tmp_flights_TAXI_IN.ARR_DELAY, tmp_imputed_ARR_DELAY.ARR_DELAY))+('||_params[11]::text||'*COALESCE(tmp_flights_TAXI_IN.DEP_DELAY, tmp_imputed_DEP_DELAY.DEP_DELAY))+
'||_params[12]::text||'*ACTUAL_ELAPSED_TIME + '||_params[13]::text||'*AIR_TIME + '||_params[14]::text||'*DEP_TIME_HOUR + '||_params[15]::text||'*DEP_TIME_MIN + '||_params[16]::text||'*WHEELS_OFF_HOUR + '||_params[17]::text||'*WHEELS_OFF_MIN + '||_params[18]::text||'*WHEELS_ON_HOUR + '||_params[19]::text||'*WHEELS_ON_MIN + '||_params[20]::text||'*ARR_TIME_HOUR + '||_params[21]::text||'*ARR_TIME_MIN + '||_params[22]::text||'*MONTH_SIN + '||_params[23]::text||'*MONTH_COS + '||_params[24]::text||'*DAY_SIN + '||_params[25]::text||'*DAY_COS + '||_params[26]::text||'*WEEKDAY_SIN + '||_params[27]::text||'*WEEKDAY_COS + '||_params[28]::text||'*EXTRA_DAY_ARR + '||_params[29]::text||'*EXTRA_DAY_DEP
FROM tmp_flights_TAXI_IN JOIN flight.schedule ON tmp_flights_TAXI_IN.flight = flight.schedule.flight
JOIN flight.distances ON flight.distances.airports = flight.schedule.airports
LEFT OUTER JOIN tmp_imputed_CRS_DEP_HOUR ON flight.schedule.flight = tmp_imputed_CRS_DEP_HOUR.flight
LEFT OUTER JOIN tmp_imputed_DISTANCE ON flight.distances.airports = tmp_imputed_DISTANCE.airports
LEFT OUTER JOIN tmp_imputed_TAXI_OUT ON tmp_flights_TAXI_IN.id = tmp_imputed_TAXI_OUT.id
LEFT OUTER JOIN tmp_imputed_DIVERTED ON tmp_flights_TAXI_IN.id = tmp_imputed_DIVERTED.id
LEFT OUTER JOIN tmp_imputed_ARR_DELAY ON tmp_flights_TAXI_IN.id = tmp_imputed_ARR_DELAY.id
LEFT OUTER JOIN tmp_imputed_DEP_DELAY ON tmp_flights_TAXI_IN.id = tmp_imputed_DEP_DELAY.id)';

                RAISE NOTICE '%', query;

                _start_ts := clock_timestamp();
                EXECUTE query;
                _end_ts   := clock_timestamp();
                RAISE NOTICE ' INSERT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _start_ts := clock_timestamp();
                EXECUTE query_cof_diff INTO _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE ' SELECT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _triple := _triple_train + _triple_removal;

        ------DIVERTED
            RAISE NOTICE '--------------DIVERTED---------------------';
            query_cof_diff := 'SELECT SUM(join_1.cof_3 * flights.cof_3) FROM
(SELECT tmp_flights_DIVERTED.flight, SUM(to_cofactor(ARRAY[(COALESCE(tmp_flights_DIVERTED.TAXI_OUT, tmp_imputed_TAXI_OUT.TAXI_OUT)), (COALESCE(tmp_flights_DIVERTED.TAXI_IN, tmp_imputed_TAXI_IN.TAXI_IN)), (tmp_imputed_DIVERTED.DIVERTED), (COALESCE(tmp_flights_DIVERTED.ARR_DELAY, tmp_imputed_ARR_DELAY.ARR_DELAY)), (COALESCE(tmp_flights_DIVERTED.DEP_DELAY, tmp_imputed_DEP_DELAY.DEP_DELAY)), (ACTUAL_ELAPSED_TIME), (AIR_TIME), (DEP_TIME_HOUR), (DEP_TIME_MIN), (WHEELS_OFF_HOUR), (WHEELS_OFF_MIN), (WHEELS_ON_HOUR), (WHEELS_ON_MIN), (ARR_TIME_HOUR), (ARR_TIME_MIN), (MONTH_SIN), (MONTH_COS), (DAY_SIN), (DAY_COS), (WEEKDAY_SIN), (WEEKDAY_COS), (EXTRA_DAY_ARR), (EXTRA_DAY_DEP)], ARRAY[]::int[])) AS cof_3 FROM
tmp_flights_DIVERTED JOIN tmp_imputed_DIVERTED ON tmp_flights_DIVERTED.id = tmp_imputed_DIVERTED.id
LEFT OUTER JOIN tmp_imputed_TAXI_OUT ON tmp_flights_DIVERTED.id = tmp_imputed_TAXI_OUT.id
LEFT OUTER JOIN tmp_imputed_TAXI_IN ON tmp_flights_DIVERTED.id = tmp_imputed_TAXI_IN.id
LEFT OUTER JOIN tmp_imputed_ARR_DELAY ON tmp_flights_DIVERTED.id = tmp_imputed_ARR_DELAY.id
LEFT OUTER JOIN tmp_imputed_DEP_DELAY ON tmp_flights_DIVERTED.id = tmp_imputed_DEP_DELAY.id
 GROUP BY tmp_flights_DIVERTED.flight
) as flights JOIN
(SELECT flight, SUM(distances.cof_1*schedule.cof_2) AS cof_3 FROM
(SELECT flight.schedule.flight, flight.schedule.airports,
SUM(to_cofactor(ARRAY[tmp_imputed_CRS_DEP_HOUR.CRS_DEP_HOUR, (CRS_DEP_MIN), (CRS_ARR_HOUR), (CRS_ARR_MIN)], ARRAY[]::int[])) as cof_2
FROM
flight.schedule LEFT OUTER JOIN tmp_imputed_CRS_DEP_HOUR ON flight.schedule.flight = tmp_imputed_CRS_DEP_HOUR.flight GROUP BY flight.schedule.flight, flight.schedule.airports) as schedule
JOIN
(SELECT flight.distances.airports, SUM(cont_to_cofactor(COALESCE(flight.distances.distance, tmp_imputed_distance.distance))) as cof_1
FROM flight.distances JOIN tmp_imputed_distance ON flight.distances.airports = tmp_imputed_distance.airports GROUP BY flight.distances.airports) as distances
ON distances.airports = schedule.airports GROUP BY schedule.flight
) as join_1
ON join_1.flight = flights.flight';
                _start_ts := clock_timestamp();
                execute(query_cof_diff) INTO _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE ' SELECT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _triple_train := _triple - _triple_removal;

                _label_idx := 7;
                _params := ridge_linear_regression(_triple_train, _label_idx, 0.001, 1, 1000);


                EXECUTE 'TRUNCATE TABLE tmp_imputed_DIVERTED';
                RAISE NOTICE ' TRUNCATE DONE ';
query := 'INSERT INTO tmp_imputed_DIVERTED
(SELECT tmp_flights_DIVERTED.id, ('||_params[1]::text || ')+(' ||_params[2]::text||'*COALESCE(flight.distances.DISTANCE, tmp_imputed_DISTANCE.DISTANCE))+('||_params[3]::text||'*COALESCE(flight.schedule.CRS_DEP_HOUR, tmp_imputed_CRS_DEP_HOUR.CRS_DEP_HOUR))+
('||_params[4]::text||'*CRS_DEP_MIN)+('||_params[5]::text||'*CRS_ARR_HOUR)+('||_params[6]::text||'*CRS_ARR_MIN)+('||_params[7]::text||'*COALESCE(tmp_flights_DIVERTED.TAXI_OUT, tmp_imputed_TAXI_OUT.TAXI_OUT))+
('||_params[8]::text||'*COALESCE(tmp_flights_DIVERTED.TAXI_IN, tmp_imputed_TAXI_IN.TAXI_IN))+('||_params[10]::text||'*COALESCE(tmp_flights_DIVERTED.ARR_DELAY, tmp_imputed_ARR_DELAY.ARR_DELAY))+('||_params[11]::text||'*COALESCE(tmp_flights_DIVERTED.DEP_DELAY, tmp_imputed_DEP_DELAY.DEP_DELAY))+
'||_params[12]::text||'*ACTUAL_ELAPSED_TIME + '||_params[13]::text||'*AIR_TIME + '||_params[14]::text||'*DEP_TIME_HOUR + '||_params[15]::text||'*DEP_TIME_MIN + '||_params[16]::text||'*WHEELS_OFF_HOUR + '||_params[17]::text||'*WHEELS_OFF_MIN + '||_params[18]::text||'*WHEELS_ON_HOUR + '||_params[19]::text||'*WHEELS_ON_MIN + '||_params[20]::text||'*ARR_TIME_HOUR + '||_params[21]::text||'*ARR_TIME_MIN + '||_params[22]::text||'*MONTH_SIN + '||_params[23]::text||'*MONTH_COS + '||_params[24]::text||'*DAY_SIN + '||_params[25]::text||'*DAY_COS + '||_params[26]::text||'*WEEKDAY_SIN + '||_params[27]::text||'*WEEKDAY_COS + '||_params[28]::text||'*EXTRA_DAY_ARR + '||_params[29]::text||'*EXTRA_DAY_DEP
FROM tmp_flights_DIVERTED JOIN flight.schedule ON tmp_flights_DIVERTED.flight = flight.schedule.flight
JOIN flight.distances ON flight.distances.airports = flight.schedule.airports
LEFT OUTER JOIN tmp_imputed_CRS_DEP_HOUR ON flight.schedule.flight = tmp_imputed_CRS_DEP_HOUR.flight
LEFT OUTER JOIN tmp_imputed_DISTANCE ON flight.distances.airports = tmp_imputed_DISTANCE.airports
LEFT OUTER JOIN tmp_imputed_TAXI_OUT ON tmp_flights_DIVERTED.id = tmp_imputed_TAXI_OUT.id
LEFT OUTER JOIN tmp_imputed_TAXI_IN ON tmp_flights_DIVERTED.id = tmp_imputed_TAXI_IN.id
LEFT OUTER JOIN tmp_imputed_ARR_DELAY ON tmp_flights_DIVERTED.id = tmp_imputed_ARR_DELAY.id
LEFT OUTER JOIN tmp_imputed_DEP_DELAY ON tmp_flights_DIVERTED.id = tmp_imputed_DEP_DELAY.id)';
                _start_ts := clock_timestamp();
                EXECUTE query;
                _end_ts   := clock_timestamp();
                RAISE NOTICE ' INSERT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _start_ts := clock_timestamp();
                EXECUTE query_cof_diff INTO _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE ' SELECT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _triple := _triple_train + _triple_removal;

                --- ARR DELAY
                RAISE NOTICE '--------------ARR DELAY---------------------';

            query_cof_diff := 'SELECT SUM(join_1.cof_3 * flights.cof_3) FROM
(SELECT tmp_flights_ARR_DELAY.flight, SUM(to_cofactor(ARRAY[(COALESCE(tmp_flights_ARR_DELAY.TAXI_OUT, tmp_imputed_TAXI_OUT.TAXI_OUT)), (COALESCE(tmp_flights_ARR_DELAY.TAXI_IN, tmp_imputed_TAXI_IN.TAXI_IN)), (COALESCE(tmp_flights_ARR_DELAY.DIVERTED, tmp_imputed_DIVERTED.DIVERTED)), (tmp_imputed_ARR_DELAY.ARR_DELAY), (COALESCE(tmp_flights_ARR_DELAY.DEP_DELAY, tmp_imputed_DEP_DELAY.DEP_DELAY)), (ACTUAL_ELAPSED_TIME), (AIR_TIME), (DEP_TIME_HOUR), (DEP_TIME_MIN), (WHEELS_OFF_HOUR), (WHEELS_OFF_MIN), (WHEELS_ON_HOUR), (WHEELS_ON_MIN), (ARR_TIME_HOUR), (ARR_TIME_MIN), (MONTH_SIN), (MONTH_COS), (DAY_SIN), (DAY_COS), (WEEKDAY_SIN), (WEEKDAY_COS), (EXTRA_DAY_ARR), (EXTRA_DAY_DEP)], ARRAY[]::int[])) AS cof_3 FROM
tmp_flights_ARR_DELAY JOIN tmp_imputed_ARR_DELAY ON tmp_flights_ARR_DELAY.id = tmp_imputed_ARR_DELAY.id
LEFT OUTER JOIN tmp_imputed_TAXI_OUT ON tmp_flights_ARR_DELAY.id = tmp_imputed_TAXI_OUT.id
LEFT OUTER JOIN tmp_imputed_TAXI_IN ON tmp_flights_ARR_DELAY.id = tmp_imputed_TAXI_IN.id
LEFT OUTER JOIN tmp_imputed_DIVERTED ON tmp_flights_ARR_DELAY.id = tmp_imputed_DIVERTED.id
LEFT OUTER JOIN tmp_imputed_DEP_DELAY ON tmp_flights_ARR_DELAY.id = tmp_imputed_DEP_DELAY.id
 GROUP BY tmp_flights_ARR_DELAY.flight
) as flights JOIN
(SELECT flight, SUM(distances.cof_1*schedule.cof_2) AS cof_3 FROM
(SELECT flight.schedule.flight, flight.schedule.airports,
SUM(to_cofactor(ARRAY[tmp_imputed_CRS_DEP_HOUR.CRS_DEP_HOUR, (CRS_DEP_MIN), (CRS_ARR_HOUR), (CRS_ARR_MIN)], ARRAY[]::int[])) as cof_2
FROM
flight.schedule LEFT OUTER JOIN tmp_imputed_CRS_DEP_HOUR ON flight.schedule.flight = tmp_imputed_CRS_DEP_HOUR.flight GROUP BY flight.schedule.flight, flight.schedule.airports) as schedule
JOIN
(SELECT flight.distances.airports, SUM(cont_to_cofactor(COALESCE(flight.distances.distance, tmp_imputed_distance.distance))) as cof_1
FROM flight.distances JOIN tmp_imputed_distance ON flight.distances.airports = tmp_imputed_distance.airports GROUP BY flight.distances.airports) as distances
ON distances.airports = schedule.airports GROUP BY schedule.flight
) as join_1
ON join_1.flight = flights.flight';
                _start_ts := clock_timestamp();
                execute(query_cof_diff) INTO _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE ' SELECT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _triple_train := _triple - _triple_removal;

                _label_idx := 8;
                _params := ridge_linear_regression(_triple_train, _label_idx, 0.001, 1, 1000);

                EXECUTE 'TRUNCATE TABLE tmp_imputed_ARR_DELAY';
query := 'INSERT INTO tmp_imputed_ARR_DELAY
(SELECT tmp_flights_ARR_DELAY.id, ('||_params[1]::text || ')+(' ||_params[2]::text||'*COALESCE(flight.distances.DISTANCE, tmp_imputed_DISTANCE.DISTANCE))+('||_params[3]::text||'*COALESCE(flight.schedule.CRS_DEP_HOUR, tmp_imputed_CRS_DEP_HOUR.CRS_DEP_HOUR))+
('||_params[4]::text||'*CRS_DEP_MIN)+('||_params[5]::text||'*CRS_ARR_HOUR)+('||_params[6]::text||'*CRS_ARR_MIN)+('||_params[7]::text||'*COALESCE(tmp_flights_ARR_DELAY.TAXI_OUT, tmp_imputed_TAXI_OUT.TAXI_OUT))+
('||_params[8]::text||'*COALESCE(tmp_flights_ARR_DELAY.TAXI_IN, tmp_imputed_TAXI_IN.TAXI_IN))+('||_params[9]::text||'*COALESCE(tmp_flights_ARR_DELAY.DIVERTED, tmp_imputed_DIVERTED.DIVERTED))+('||_params[11]::text||'*COALESCE(tmp_flights_ARR_DELAY.DEP_DELAY, tmp_imputed_DEP_DELAY.DEP_DELAY))+
'||_params[12]::text||'*ACTUAL_ELAPSED_TIME + '||_params[13]::text||'*AIR_TIME + '||_params[14]::text||'*DEP_TIME_HOUR + '||_params[15]::text||'*DEP_TIME_MIN + '||_params[16]::text||'*WHEELS_OFF_HOUR + '||_params[17]::text||'*WHEELS_OFF_MIN + '||_params[18]::text||'*WHEELS_ON_HOUR + '||_params[19]::text||'*WHEELS_ON_MIN + '||_params[20]::text||'*ARR_TIME_HOUR + '||_params[21]::text||'*ARR_TIME_MIN + '||_params[22]::text||'*MONTH_SIN + '||_params[23]::text||'*MONTH_COS + '||_params[24]::text||'*DAY_SIN + '||_params[25]::text||'*DAY_COS + '||_params[26]::text||'*WEEKDAY_SIN + '||_params[27]::text||'*WEEKDAY_COS + '||_params[28]::text||'*EXTRA_DAY_ARR + '||_params[29]::text||'*EXTRA_DAY_DEP
FROM tmp_flights_ARR_DELAY JOIN flight.schedule ON tmp_flights_ARR_DELAY.flight = flight.schedule.flight
JOIN flight.distances ON flight.distances.airports = flight.schedule.airports
LEFT OUTER JOIN tmp_imputed_CRS_DEP_HOUR ON flight.schedule.flight = tmp_imputed_CRS_DEP_HOUR.flight
LEFT OUTER JOIN tmp_imputed_DISTANCE ON flight.distances.airports = tmp_imputed_DISTANCE.airports
LEFT OUTER JOIN tmp_imputed_TAXI_OUT ON tmp_flights_ARR_DELAY.id = tmp_imputed_TAXI_OUT.id
LEFT OUTER JOIN tmp_imputed_TAXI_IN ON tmp_flights_ARR_DELAY.id = tmp_imputed_TAXI_IN.id
LEFT OUTER JOIN tmp_imputed_DIVERTED ON tmp_flights_ARR_DELAY.id = tmp_imputed_DIVERTED.id
LEFT OUTER JOIN tmp_imputed_DEP_DELAY ON tmp_flights_ARR_DELAY.id = tmp_imputed_DEP_DELAY.id)';
                _start_ts := clock_timestamp();
                EXECUTE query;
                _end_ts   := clock_timestamp();
                RAISE NOTICE ' INSERT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _start_ts := clock_timestamp();
                EXECUTE query_cof_diff INTO _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE ' SELECT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _triple := _triple_train + _triple_removal;

                --- DEP_DELAY ----
                RAISE NOTICE '--------------DEP DELAY---------------------';


            query_cof_diff := 'SELECT SUM(join_1.cof_3 * flights.cof_3) FROM
(SELECT tmp_flights_DEP_DELAY.flight, SUM(to_cofactor(ARRAY[(COALESCE(tmp_flights_DEP_DELAY.TAXI_OUT, tmp_imputed_TAXI_OUT.TAXI_OUT)), (COALESCE(tmp_flights_DEP_DELAY.TAXI_IN, tmp_imputed_TAXI_IN.TAXI_IN)), (COALESCE(tmp_flights_DEP_DELAY.DIVERTED, tmp_imputed_DIVERTED.DIVERTED)), (COALESCE(tmp_flights_DEP_DELAY.ARR_DELAY, tmp_imputed_ARR_DELAY.ARR_DELAY)), (tmp_imputed_DEP_DELAY.DEP_DELAY), (ACTUAL_ELAPSED_TIME), (AIR_TIME), (DEP_TIME_HOUR), (DEP_TIME_MIN), (WHEELS_OFF_HOUR), (WHEELS_OFF_MIN), (WHEELS_ON_HOUR), (WHEELS_ON_MIN), (ARR_TIME_HOUR), (ARR_TIME_MIN), (MONTH_SIN), (MONTH_COS), (DAY_SIN), (DAY_COS), (WEEKDAY_SIN), (WEEKDAY_COS), (EXTRA_DAY_ARR), (EXTRA_DAY_DEP)], ARRAY[]::int[])) AS cof_3 FROM
tmp_flights_DEP_DELAY JOIN tmp_imputed_DEP_DELAY ON tmp_flights_DEP_DELAY.id = tmp_imputed_DEP_DELAY.id
LEFT OUTER JOIN tmp_imputed_TAXI_OUT ON tmp_flights_DEP_DELAY.id = tmp_imputed_TAXI_OUT.id
LEFT OUTER JOIN tmp_imputed_TAXI_IN ON tmp_flights_DEP_DELAY.id = tmp_imputed_TAXI_IN.id
LEFT OUTER JOIN tmp_imputed_DIVERTED ON tmp_flights_DEP_DELAY.id = tmp_imputed_DIVERTED.id
LEFT OUTER JOIN tmp_imputed_ARR_DELAY ON tmp_flights_DEP_DELAY.id = tmp_imputed_ARR_DELAY.id
 GROUP BY tmp_flights_DEP_DELAY.flight
) as flights JOIN
(SELECT flight, SUM(distances.cof_1*schedule.cof_2) AS cof_3 FROM
(SELECT flight.schedule.flight, flight.schedule.airports,
SUM(to_cofactor(ARRAY[tmp_imputed_CRS_DEP_HOUR.CRS_DEP_HOUR, (CRS_DEP_MIN), (CRS_ARR_HOUR), (CRS_ARR_MIN)], ARRAY[]::int[])) as cof_2
FROM
flight.schedule LEFT OUTER JOIN tmp_imputed_CRS_DEP_HOUR ON flight.schedule.flight = tmp_imputed_CRS_DEP_HOUR.flight GROUP BY flight.schedule.flight, flight.schedule.airports) as schedule
JOIN
(SELECT flight.distances.airports, SUM(cont_to_cofactor(COALESCE(flight.distances.distance, tmp_imputed_distance.distance))) as cof_1
FROM flight.distances JOIN tmp_imputed_distance ON flight.distances.airports = tmp_imputed_distance.airports GROUP BY flight.distances.airports) as distances
ON distances.airports = schedule.airports GROUP BY schedule.flight
) as join_1
ON join_1.flight = flights.flight';
                _start_ts := clock_timestamp();
                execute(query_cof_diff) INTO _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE ' SELECT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _triple_train := _triple - _triple_removal;

                _label_idx := 9;
                _params := ridge_linear_regression(_triple_train, _label_idx, 0.001, 1, 1000);


                EXECUTE 'TRUNCATE TABLE tmp_imputed_DEP_DELAY';
query := 'INSERT INTO tmp_imputed_DEP_DELAY
(SELECT tmp_flights_DEP_DELAY.id, ('||_params[1]::text || ')+(' ||_params[2]::text||'*COALESCE(flight.distances.DISTANCE, tmp_imputed_DISTANCE.DISTANCE))+('||_params[3]::text||'*COALESCE(flight.schedule.CRS_DEP_HOUR, tmp_imputed_CRS_DEP_HOUR.CRS_DEP_HOUR))+
('||_params[4]::text||'*CRS_DEP_MIN)+('||_params[5]::text||'*CRS_ARR_HOUR)+('||_params[6]::text||'*CRS_ARR_MIN)+('||_params[7]::text||'*COALESCE(tmp_flights_DEP_DELAY.TAXI_OUT, tmp_imputed_TAXI_OUT.TAXI_OUT))+
('||_params[8]::text||'*COALESCE(tmp_flights_DEP_DELAY.TAXI_IN, tmp_imputed_TAXI_IN.TAXI_IN))+('||_params[9]::text||'*COALESCE(tmp_flights_DEP_DELAY.DIVERTED, tmp_imputed_DIVERTED.DIVERTED))+('||_params[10]::text||'*COALESCE(tmp_flights_DEP_DELAY.ARR_DELAY, tmp_imputed_ARR_DELAY.ARR_DELAY))+
'||_params[12]::text||'*ACTUAL_ELAPSED_TIME + '||_params[13]::text||'*AIR_TIME + '||_params[14]::text||'*DEP_TIME_HOUR + '||_params[15]::text||'*DEP_TIME_MIN + '||_params[16]::text||'*WHEELS_OFF_HOUR + '||_params[17]::text||'*WHEELS_OFF_MIN + '||_params[18]::text||'*WHEELS_ON_HOUR + '||_params[19]::text||'*WHEELS_ON_MIN + '||_params[20]::text||'*ARR_TIME_HOUR + '||_params[21]::text||'*ARR_TIME_MIN + '||_params[22]::text||'*MONTH_SIN + '||_params[23]::text||'*MONTH_COS + '||_params[24]::text||'*DAY_SIN + '||_params[25]::text||'*DAY_COS + '||_params[26]::text||'*WEEKDAY_SIN + '||_params[27]::text||'*WEEKDAY_COS + '||_params[28]::text||'*EXTRA_DAY_ARR + '||_params[29]::text||'*EXTRA_DAY_DEP
FROM tmp_flights_DEP_DELAY JOIN flight.schedule ON tmp_flights_DEP_DELAY.flight = flight.schedule.flight
JOIN flight.distances ON flight.distances.airports = flight.schedule.airports
LEFT OUTER JOIN tmp_imputed_CRS_DEP_HOUR ON flight.schedule.flight = tmp_imputed_CRS_DEP_HOUR.flight
LEFT OUTER JOIN tmp_imputed_DISTANCE ON flight.distances.airports = tmp_imputed_DISTANCE.airports
LEFT OUTER JOIN tmp_imputed_TAXI_OUT ON tmp_flights_DEP_DELAY.id = tmp_imputed_TAXI_OUT.id
LEFT OUTER JOIN tmp_imputed_TAXI_IN ON tmp_flights_DEP_DELAY.id = tmp_imputed_TAXI_IN.id
LEFT OUTER JOIN tmp_imputed_DIVERTED ON tmp_flights_DEP_DELAY.id = tmp_imputed_DIVERTED.id
LEFT OUTER JOIN tmp_imputed_ARR_DELAY ON tmp_flights_DEP_DELAY.id = tmp_imputed_ARR_DELAY.id)';
                _start_ts := clock_timestamp();
                EXECUTE query;
                                _end_ts   := clock_timestamp();
                RAISE NOTICE ' INSERT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                                _start_ts := clock_timestamp();
                EXECUTE query_cof_diff INTO _triple_removal;
                                _end_ts   := clock_timestamp();
                RAISE NOTICE ' SELECT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _triple := _triple_train + _triple_removal;
        --end loop;
    end loop;
    RETURN 0;
  END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION main() RETURNS int AS $$
  DECLARE
      tbls t_info[];
      t_impute t_info;
      join_tables text[];
  BEGIN
      --define tables []
    tbls := tbls || row('distances', 'flight', '{}', '{"DISTANCE"}', '{"airports"}', '{}')::t_info;
    tbls := tbls || row('schedule', 'flight', '{"CRS_DEP_MIN","CRS_ARR_HOUR","CRS_ARR_MIN"}', '{"CRS_DEP_HOUR"}', '{"flight"}', '{"airports"}')::t_info;
    tbls := tbls || row('flights', 'flight', '{"ACTUAL_ELAPSED_TIME","AIR_TIME","DEP_TIME_HOUR","DEP_TIME_MIN","WHEELS_OFF_HOUR","WHEELS_OFF_MIN","WHEELS_ON_HOUR","WHEELS_ON_MIN","ARR_TIME_HOUR","ARR_TIME_MIN","MONTH_SIN","MONTH_COS","DAY_SIN","DAY_COS","WEEKDAY_SIN","WEEKDAY_COS","EXTRA_DAY_ARR","EXTRA_DAY_DEP"}', '{"TAXI_OUT","TAXI_IN", "DIVERTED", "ARR_DELAY","DEP_DELAY"}', '{"id"}', '{"flight"}')::t_info;
    join_tables := ARRAY['Inventory.locn = Location.locn', 'Inventory.locn = Weather.locn AND Inventory.dateid = Weather.dateid', 'Inventory.ksn = Retailer.ksn', 'Census.zip = Location.zip'];

    return impute(tbls,2);
  END;
$$ LANGUAGE plpgsql;
