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
    EXECUTE format('ALTER TABLE tmp_%s_%s SET (parallel_workers = 20)', tbl.name, col_nan);

    RAISE NOTICE '%', format('CREATE INDEX idx_tmp_id_%s_%s ON tmp_%s_%s USING btree (%s)', VARIADIC ARRAY[tbl.name, col_nan] || tbl.name || col_nan || tbl.identifier);
    EXECUTE format('CREATE INDEX idx_tmp_id_%s_%s ON tmp_%s_%s USING btree (%s)', VARIADIC ARRAY[tbl.name, col_nan] || tbl.name || col_nan || tbl.identifier);
    IF cardinality(tbl.join_keys) > 0 then
        RAISE NOTICE '%', format('CREATE INDEX idx_tmp_%s_%s ON tmp_%s_%s USING btree (%s)', VARIADIC ARRAY[tbl.name, col_nan] || tbl.name || col_nan || tbl.join_keys[1]);
        EXECUTE format('CREATE INDEX idx_tmp_%s_%s ON tmp_%s_%s USING btree (%s)', VARIADIC ARRAY[tbl.name, col_nan] || tbl.name || col_nan || tbl.join_keys[1]);
    end if;
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
    EXECUTE format('ALTER TABLE tmp_imputed_%s SET (parallel_workers = 20)', col_nan);

    RAISE NOTICE 'create index idx_imputed_% ON tmp_imputed_% USING btree (%)', col_nan, col_nan, tbl.identifier[1];
    EXECUTE format('create index idx_imputed_%s ON tmp_imputed_%s USING btree (%s)', VARIADIC ARRAY[col_nan, col_nan, tbl.identifier[1]]);
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
      query text := '';
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
        --queries := queries || ('(SELECT ' || group_by || ', SUM(to_cofactor(ARRAY[');
        _imputed_data := standard_imputation(tbl);
        ---create tmp table missing values and imputed values
        FOR _counter in 1..cardinality(tbl.columns_null)
        LOOP
            perform create_table_missing_values(tbl,  tbl.columns_null[_counter]);
            perform create_table_imputed_values(tbl, tbl.columns_null[_counter], _imputed_data[_counter]);
            query := query || format('COALESCE(%s, %s), ', tbl.columns_null[_counter], _imputed_data[_counter]);
        end loop;
        --queries[index] := rtrim(queries[index], ', ') || '], ARRAY[';

        FOR _counter in 1..cardinality(tbl.columns_no_null)
        LOOP
            query := query || format('%s, ', tbl.columns_no_null[_counter]);
        end loop;
        --queries[index] := rtrim(queries[index], ', ') || '], ARRAY[]::int[]))';

        ---add group by
        --queries[index] := queries[index] || format(' AS cof_%s FROM %s.%s GROUP BY %s) AS %s', index, tbl.schema, tbl.name, group_by, tbl.name);
        index = index + 1;
    END LOOP;

    --distances, schedule, flight
    query := format('SELECT SUM(to_cofactor(ARRAY[%s],ARRAY[]::int[])) from flight.distances join flight.schedule ON flight.distances.airports = flight.schedule.airports join flight.flights on flight.flights.flight = flight.schedule.flight; ', rtrim(query, ', '));
    --query := format('SELECT SUM(join_1.cof_3 * flights.cof_3) FROM (%s JOIN %s ON join_1.flight = flights.flight', query, queries[3]);

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
                query_cof_diff := 'SELECT SUM(to_cofactor(ARRAY[tmp_imputed_distance.distance, COALESCE(flight.schedule.CRS_DEP_HOUR, tmp_imputed_CRS_DEP_HOUR.CRS_DEP_HOUR), (CRS_DEP_MIN), (CRS_ARR_HOUR), (CRS_ARR_MIN),COALESCE(flight.flights.TAXI_OUT, tmp_imputed_TAXI_OUT.TAXI_OUT), (COALESCE(flight.flights.TAXI_IN, tmp_imputed_TAXI_IN.TAXI_IN)), (COALESCE(flight.flights.DIVERTED, tmp_imputed_DIVERTED.DIVERTED)), (COALESCE(flight.flights.ARR_DELAY, tmp_imputed_ARR_DELAY.ARR_DELAY)), (COALESCE(flight.flights.DEP_DELAY, tmp_imputed_DEP_DELAY.DEP_DELAY)), (ACTUAL_ELAPSED_TIME), (AIR_TIME), (DEP_TIME_HOUR), (DEP_TIME_MIN), (WHEELS_OFF_HOUR), (WHEELS_OFF_MIN), (WHEELS_ON_HOUR), (WHEELS_ON_MIN), (ARR_TIME_HOUR), (ARR_TIME_MIN), (MONTH_SIN), (MONTH_COS), (DAY_SIN), (DAY_COS), (WEEKDAY_SIN), (WEEKDAY_COS), (EXTRA_DAY_ARR), (EXTRA_DAY_DEP)], ARRAY[]::int[]))
FROM
tmp_distances_DISTANCE JOIN tmp_imputed_distance ON tmp_distances_DISTANCE.airports = tmp_imputed_distance.airports
JOIN flight.schedule ON tmp_distances_DISTANCE.airports = flight.schedule.airports
LEFT OUTER JOIN tmp_imputed_CRS_DEP_HOUR ON flight.schedule.flight = tmp_imputed_CRS_DEP_HOUR.flight
JOIN flight.flights ON flight.flights.flight = flight.schedule.flight
LEFT OUTER JOIN tmp_imputed_TAXI_OUT ON flight.flights.id = tmp_imputed_TAXI_OUT.id
LEFT OUTER JOIN tmp_imputed_TAXI_IN ON flight.flights.id = tmp_imputed_TAXI_IN.id
LEFT OUTER JOIN tmp_imputed_DIVERTED ON flight.flights.id = tmp_imputed_DIVERTED.id
LEFT OUTER JOIN tmp_imputed_ARR_DELAY ON flight.flights.id = tmp_imputed_ARR_DELAY.id
LEFT OUTER JOIN tmp_imputed_DEP_DELAY ON flight.flights.id = tmp_imputed_DEP_DELAY.id';
                _start_ts := clock_timestamp();
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
                EXECUTE 'DROP INDEX idx_imputed_DISTANCE;';
                EXECUTE 'TRUNCATE TABLE tmp_imputed_distance';

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
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Impute new values and update cofactor matrix: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));

                EXECUTE 'create index idx_imputed_DISTANCE ON tmp_imputed_DISTANCE USING btree (airports)';
                _start_ts := clock_timestamp();
                EXECUTE query_cof_diff INTO _triple_removal;
                _triple := _triple_train + _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Impute new values and update cofactor matrix: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));

                ---CRS_DEP_HOUR
                RAISE NOTICE '--------------IMPUTING CRS DEP HOUR---------------------';
                query_cof_diff := 'SELECT SUM(to_cofactor(ARRAY[COALESCE(flight.distances.distance, tmp_imputed_distance.distance), tmp_imputed_CRS_DEP_HOUR.CRS_DEP_HOUR, (CRS_DEP_MIN), (CRS_ARR_HOUR), (CRS_ARR_MIN),COALESCE(flight.flights.TAXI_OUT, tmp_imputed_TAXI_OUT.TAXI_OUT), (COALESCE(flight.flights.TAXI_IN, tmp_imputed_TAXI_IN.TAXI_IN)), (COALESCE(flight.flights.DIVERTED, tmp_imputed_DIVERTED.DIVERTED)), (COALESCE(flight.flights.ARR_DELAY, tmp_imputed_ARR_DELAY.ARR_DELAY)), (COALESCE(flight.flights.DEP_DELAY, tmp_imputed_DEP_DELAY.DEP_DELAY)), (ACTUAL_ELAPSED_TIME), (AIR_TIME), (DEP_TIME_HOUR), (DEP_TIME_MIN), (WHEELS_OFF_HOUR), (WHEELS_OFF_MIN), (WHEELS_ON_HOUR), (WHEELS_ON_MIN), (ARR_TIME_HOUR), (ARR_TIME_MIN), (MONTH_SIN), (MONTH_COS), (DAY_SIN), (DAY_COS), (WEEKDAY_SIN), (WEEKDAY_COS), (EXTRA_DAY_ARR), (EXTRA_DAY_DEP)], ARRAY[]::int[]))
FROM
tmp_schedule_CRS_DEP_HOUR JOIN tmp_imputed_CRS_DEP_HOUR ON tmp_schedule_CRS_DEP_HOUR.flight = tmp_imputed_CRS_DEP_HOUR.flight
JOIN flight.distances ON tmp_schedule_CRS_DEP_HOUR.airports = flight.distances.airports
LEFT OUTER JOIN tmp_imputed_distance ON flight.distances.airports = tmp_imputed_distance.airports
JOIN flight.flights ON flight.flights.flight = tmp_schedule_CRS_DEP_HOUR.flight
LEFT OUTER JOIN tmp_imputed_TAXI_OUT ON flight.flights.id = tmp_imputed_TAXI_OUT.id
LEFT OUTER JOIN tmp_imputed_TAXI_IN ON flight.flights.id = tmp_imputed_TAXI_IN.id
LEFT OUTER JOIN tmp_imputed_DIVERTED ON flight.flights.id = tmp_imputed_DIVERTED.id
LEFT OUTER JOIN tmp_imputed_ARR_DELAY ON flight.flights.id = tmp_imputed_ARR_DELAY.id
LEFT OUTER JOIN tmp_imputed_DEP_DELAY ON flight.flights.id = tmp_imputed_DEP_DELAY.id';
                _start_ts := clock_timestamp();
                execute(query_cof_diff) INTO _triple_removal;
                _triple_train := _triple - _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE ' SELECT: time: %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _label_idx := 1;
                _params := ridge_linear_regression(_triple_train, _label_idx, 0.001, 1, 1000);
                EXECUTE 'DROP INDEX idx_imputed_CRS_DEP_HOUR;';
                EXECUTE 'TRUNCATE TABLE tmp_imputed_CRS_DEP_HOUR';
                RAISE NOTICE ' TRUNCATE DONE ';

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
                EXECUTE 'create index idx_imputed_CRS_DEP_HOUR ON tmp_imputed_CRS_DEP_HOUR USING btree (flight)';
                _end_ts   := clock_timestamp();
                RAISE NOTICE ' INSERT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _start_ts := clock_timestamp();
                EXECUTE query_cof_diff INTO _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE ' SELECT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _triple := _triple_train + _triple_removal;
                ---taxi out
                RAISE NOTICE '--------------TAXI OUT---------------------';
                query_cof_diff := 'SELECT SUM(to_cofactor(ARRAY[COALESCE(flight.distances.distance, tmp_imputed_distance.distance), COALESCE(flight.schedule.CRS_DEP_HOUR, tmp_imputed_CRS_DEP_HOUR.CRS_DEP_HOUR), (CRS_DEP_MIN), (CRS_ARR_HOUR), (CRS_ARR_MIN), tmp_imputed_TAXI_OUT.TAXI_OUT, (COALESCE(tmp_flights_TAXI_OUT.TAXI_IN, tmp_imputed_TAXI_IN.TAXI_IN)), (COALESCE(tmp_flights_TAXI_OUT.DIVERTED, tmp_imputed_DIVERTED.DIVERTED)), (COALESCE(tmp_flights_TAXI_OUT.ARR_DELAY, tmp_imputed_ARR_DELAY.ARR_DELAY)), (COALESCE(tmp_flights_TAXI_OUT.DEP_DELAY, tmp_imputed_DEP_DELAY.DEP_DELAY)), (ACTUAL_ELAPSED_TIME), (AIR_TIME), (DEP_TIME_HOUR), (DEP_TIME_MIN), (WHEELS_OFF_HOUR), (WHEELS_OFF_MIN), (WHEELS_ON_HOUR), (WHEELS_ON_MIN), (ARR_TIME_HOUR), (ARR_TIME_MIN), (MONTH_SIN), (MONTH_COS), (DAY_SIN), (DAY_COS), (WEEKDAY_SIN), (WEEKDAY_COS), (EXTRA_DAY_ARR), (EXTRA_DAY_DEP)], ARRAY[]::int[]))
FROM
tmp_flights_TAXI_OUT JOIN tmp_imputed_TAXI_OUT ON tmp_flights_TAXI_OUT.id = tmp_imputed_TAXI_OUT.id
JOIN flight.schedule ON tmp_flights_TAXI_OUT.flight = flight.schedule.flight
LEFT OUTER JOIN tmp_imputed_CRS_DEP_HOUR ON tmp_flights_TAXI_OUT.flight = tmp_imputed_CRS_DEP_HOUR.flight
JOIN flight.distances ON flight.distances.airports = flight.schedule.airports
LEFT OUTER JOIN tmp_imputed_distance ON flight.distances.airports = tmp_imputed_distance.airports
LEFT OUTER JOIN tmp_imputed_TAXI_IN ON tmp_flights_TAXI_OUT.id = tmp_imputed_TAXI_IN.id
LEFT OUTER JOIN tmp_imputed_DIVERTED ON tmp_flights_TAXI_OUT.id = tmp_imputed_DIVERTED.id
LEFT OUTER JOIN tmp_imputed_ARR_DELAY ON tmp_flights_TAXI_OUT.id = tmp_imputed_ARR_DELAY.id
LEFT OUTER JOIN tmp_imputed_DEP_DELAY ON tmp_flights_TAXI_OUT.id = tmp_imputed_DEP_DELAY.id';
                _start_ts := clock_timestamp();
                execute(query_cof_diff) INTO _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE ' SELECT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _triple_train := _triple - _triple_removal;

                _label_idx := 5;
                _params := ridge_linear_regression(_triple_train, _label_idx, 0.001, 1, 1000);
                EXECUTE 'DROP INDEX idx_imputed_TAXI_OUT;';
                EXECUTE 'TRUNCATE TABLE tmp_imputed_TAXI_OUT';
                RAISE NOTICE ' TRUNCATE DONE ';

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
                EXECUTE 'create index idx_imputed_TAXI_OUT ON tmp_imputed_TAXI_OUT USING btree (id)';
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
            query_cof_diff := 'SELECT SUM(to_cofactor(ARRAY[COALESCE(flight.distances.distance, tmp_imputed_distance.distance), COALESCE(flight.schedule.CRS_DEP_HOUR, tmp_imputed_CRS_DEP_HOUR.CRS_DEP_HOUR), (CRS_DEP_MIN), (CRS_ARR_HOUR), (CRS_ARR_MIN), (COALESCE(tmp_flights_TAXI_IN.TAXI_OUT, tmp_imputed_TAXI_OUT.TAXI_OUT)), tmp_imputed_TAXI_IN.TAXI_IN, (COALESCE(tmp_flights_TAXI_IN.DIVERTED, tmp_imputed_DIVERTED.DIVERTED)), (COALESCE(tmp_flights_TAXI_IN.ARR_DELAY, tmp_imputed_ARR_DELAY.ARR_DELAY)), (COALESCE(tmp_flights_TAXI_IN.DEP_DELAY, tmp_imputed_DEP_DELAY.DEP_DELAY)), (ACTUAL_ELAPSED_TIME), (AIR_TIME), (DEP_TIME_HOUR), (DEP_TIME_MIN), (WHEELS_OFF_HOUR), (WHEELS_OFF_MIN), (WHEELS_ON_HOUR), (WHEELS_ON_MIN), (ARR_TIME_HOUR), (ARR_TIME_MIN), (MONTH_SIN), (MONTH_COS), (DAY_SIN), (DAY_COS), (WEEKDAY_SIN), (WEEKDAY_COS), (EXTRA_DAY_ARR), (EXTRA_DAY_DEP)], ARRAY[]::int[]))
FROM
tmp_flights_TAXI_IN JOIN tmp_imputed_TAXI_IN ON tmp_flights_TAXI_IN.id = tmp_imputed_TAXI_IN.id
JOIN flight.schedule ON tmp_flights_TAXI_IN.flight = flight.schedule.flight
LEFT OUTER JOIN tmp_imputed_CRS_DEP_HOUR ON tmp_flights_TAXI_IN.flight = tmp_imputed_CRS_DEP_HOUR.flight
JOIN flight.distances ON flight.distances.airports = flight.schedule.airports
LEFT OUTER JOIN tmp_imputed_DISTANCE ON flight.distances.airports = tmp_imputed_distance.airports
LEFT OUTER JOIN tmp_imputed_TAXI_OUT ON tmp_flights_TAXI_IN.id = tmp_imputed_TAXI_OUT.id
LEFT OUTER JOIN tmp_imputed_DIVERTED ON tmp_flights_TAXI_IN.id = tmp_imputed_DIVERTED.id
LEFT OUTER JOIN tmp_imputed_ARR_DELAY ON tmp_flights_TAXI_IN.id = tmp_imputed_ARR_DELAY.id
LEFT OUTER JOIN tmp_imputed_DEP_DELAY ON tmp_flights_TAXI_IN.id = tmp_imputed_DEP_DELAY.id';
                _start_ts := clock_timestamp();
                execute(query_cof_diff) INTO _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE ' SELECT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _triple_train := _triple - _triple_removal;

                _label_idx := 6;
                _params := ridge_linear_regression(_triple_train, _label_idx, 0.001, 1, 1000);
                RAISE NOTICE 'params %', _params;
                EXECUTE 'DROP INDEX idx_imputed_TAXI_IN;';
                EXECUTE 'TRUNCATE TABLE tmp_imputed_TAXI_IN';
                RAISE NOTICE ' TRUNCATE DONE ';

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
                EXECUTE 'create index idx_imputed_TAXI_IN ON tmp_imputed_TAXI_IN USING btree (id)';
                _end_ts   := clock_timestamp();
                RAISE NOTICE ' INSERT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _start_ts := clock_timestamp();
                EXECUTE query_cof_diff INTO _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE ' SELECT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _triple := _triple_train + _triple_removal;

        ------DIVERTED
            RAISE NOTICE '--------------DIVERTED---------------------';
            query_cof_diff := 'SELECT SUM(to_cofactor(ARRAY[COALESCE(flight.distances.distance, tmp_imputed_distance.distance), COALESCE(flight.schedule.CRS_DEP_HOUR, tmp_imputed_CRS_DEP_HOUR.CRS_DEP_HOUR), (CRS_DEP_MIN), (CRS_ARR_HOUR), (CRS_ARR_MIN), (COALESCE(tmp_flights_DIVERTED.TAXI_OUT, tmp_imputed_TAXI_OUT.TAXI_OUT)), (COALESCE(tmp_flights_DIVERTED.TAXI_IN, tmp_imputed_TAXI_IN.TAXI_IN)), tmp_imputed_DIVERTED.DIVERTED, (COALESCE(tmp_flights_DIVERTED.ARR_DELAY, tmp_imputed_ARR_DELAY.ARR_DELAY)), (COALESCE(tmp_flights_DIVERTED.DEP_DELAY, tmp_imputed_DEP_DELAY.DEP_DELAY)), (ACTUAL_ELAPSED_TIME), (AIR_TIME), (DEP_TIME_HOUR), (DEP_TIME_MIN), (WHEELS_OFF_HOUR), (WHEELS_OFF_MIN), (WHEELS_ON_HOUR), (WHEELS_ON_MIN), (ARR_TIME_HOUR), (ARR_TIME_MIN), (MONTH_SIN), (MONTH_COS), (DAY_SIN), (DAY_COS), (WEEKDAY_SIN), (WEEKDAY_COS), (EXTRA_DAY_ARR), (EXTRA_DAY_DEP)], ARRAY[]::int[]))
FROM
tmp_flights_DIVERTED JOIN tmp_imputed_DIVERTED ON tmp_flights_DIVERTED.id = tmp_imputed_DIVERTED.id
JOIN flight.schedule ON tmp_flights_DIVERTED.flight = flight.schedule.flight
LEFT OUTER JOIN tmp_imputed_CRS_DEP_HOUR ON tmp_flights_DIVERTED.flight = tmp_imputed_CRS_DEP_HOUR.flight
JOIN flight.distances ON flight.distances.airports = flight.schedule.airports
LEFT OUTER JOIN tmp_imputed_TAXI_OUT ON tmp_flights_DIVERTED.id = tmp_imputed_TAXI_OUT.id
LEFT OUTER JOIN tmp_imputed_TAXI_IN ON tmp_flights_DIVERTED.id = tmp_imputed_TAXI_IN.id
LEFT OUTER JOIN tmp_imputed_DISTANCE ON flight.distances.airports = tmp_imputed_DISTANCE.airports
LEFT OUTER JOIN tmp_imputed_ARR_DELAY ON tmp_flights_DIVERTED.id = tmp_imputed_ARR_DELAY.id
LEFT OUTER JOIN tmp_imputed_DEP_DELAY ON tmp_flights_DIVERTED.id = tmp_imputed_DEP_DELAY.id';
                _start_ts := clock_timestamp();
                execute(query_cof_diff) INTO _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE ' SELECT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _triple_train := _triple - _triple_removal;

                _label_idx := 7;
                _params := ridge_linear_regression(_triple_train, _label_idx, 0.001, 1, 1000);
                EXECUTE 'DROP INDEX idx_imputed_DIVERTED;';
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
                EXECUTE 'create index idx_imputed_DIVERTED ON tmp_imputed_DIVERTED USING btree (id)';
                _end_ts   := clock_timestamp();
                RAISE NOTICE ' INSERT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _start_ts := clock_timestamp();
                EXECUTE query_cof_diff INTO _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE ' SELECT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _triple := _triple_train + _triple_removal;

                --- ARR DELAY
                RAISE NOTICE '--------------ARR DELAY---------------------';

            query_cof_diff := 'SELECT SUM(to_cofactor(ARRAY[COALESCE(flight.distances.distance, tmp_imputed_distance.distance), COALESCE(flight.schedule.CRS_DEP_HOUR, tmp_imputed_CRS_DEP_HOUR.CRS_DEP_HOUR), (CRS_DEP_MIN), (CRS_ARR_HOUR), (CRS_ARR_MIN), (COALESCE(tmp_flights_ARR_DELAY.TAXI_OUT, tmp_imputed_TAXI_OUT.TAXI_OUT)), (COALESCE(tmp_flights_ARR_DELAY.TAXI_IN, tmp_imputed_TAXI_IN.TAXI_IN)), (COALESCE(tmp_flights_ARR_DELAY.DIVERTED, tmp_imputed_DIVERTED.DIVERTED)), tmp_imputed_ARR_DELAY.ARR_DELAY, (COALESCE(tmp_flights_ARR_DELAY.DEP_DELAY, tmp_imputed_DEP_DELAY.DEP_DELAY)), (ACTUAL_ELAPSED_TIME), (AIR_TIME), (DEP_TIME_HOUR), (DEP_TIME_MIN), (WHEELS_OFF_HOUR), (WHEELS_OFF_MIN), (WHEELS_ON_HOUR), (WHEELS_ON_MIN), (ARR_TIME_HOUR), (ARR_TIME_MIN), (MONTH_SIN), (MONTH_COS), (DAY_SIN), (DAY_COS), (WEEKDAY_SIN), (WEEKDAY_COS), (EXTRA_DAY_ARR), (EXTRA_DAY_DEP)], ARRAY[]::int[]))
FROM
tmp_flights_ARR_DELAY JOIN tmp_imputed_ARR_DELAY ON tmp_flights_ARR_DELAY.id = tmp_imputed_ARR_DELAY.id
JOIN flight.schedule ON tmp_flights_ARR_DELAY.flight = flight.schedule.flight
LEFT OUTER JOIN tmp_imputed_CRS_DEP_HOUR ON tmp_flights_ARR_DELAY.flight = tmp_imputed_CRS_DEP_HOUR.flight
JOIN flight.distances ON flight.distances.airports = flight.schedule.airports
LEFT OUTER JOIN tmp_imputed_TAXI_OUT ON tmp_flights_ARR_DELAY.id = tmp_imputed_TAXI_OUT.id
LEFT OUTER JOIN tmp_imputed_TAXI_IN ON tmp_flights_ARR_DELAY.id = tmp_imputed_TAXI_IN.id
LEFT OUTER JOIN tmp_imputed_DISTANCE ON flight.distances.airports = tmp_imputed_DISTANCE.airports
LEFT OUTER JOIN tmp_imputed_DIVERTED ON tmp_flights_ARR_DELAY.id = tmp_imputed_DIVERTED.id
LEFT OUTER JOIN tmp_imputed_DEP_DELAY ON tmp_flights_ARR_DELAY.id = tmp_imputed_DEP_DELAY.id';
                _start_ts := clock_timestamp();
                execute(query_cof_diff) INTO _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE ' SELECT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _triple_train := _triple - _triple_removal;

                _label_idx := 8;
                _params := ridge_linear_regression(_triple_train, _label_idx, 0.001, 1, 1000);
                EXECUTE 'DROP INDEX idx_imputed_ARR_DELAY;';
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
                EXECUTE 'create index idx_imputed_ARR_DELAY ON tmp_imputed_ARR_DELAY USING btree (id)';
                _end_ts   := clock_timestamp();
                RAISE NOTICE ' INSERT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _start_ts := clock_timestamp();
                EXECUTE query_cof_diff INTO _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE ' SELECT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _triple := _triple_train + _triple_removal;

                --- DEP_DELAY ----
                RAISE NOTICE '--------------DEP DELAY---------------------';


            query_cof_diff := 'SELECT SUM(to_cofactor(ARRAY[COALESCE(flight.distances.distance, tmp_imputed_distance.distance), COALESCE(flight.schedule.CRS_DEP_HOUR, tmp_imputed_CRS_DEP_HOUR.CRS_DEP_HOUR), (CRS_DEP_MIN), (CRS_ARR_HOUR), (CRS_ARR_MIN), (COALESCE(tmp_flights_DEP_DELAY.TAXI_OUT, tmp_imputed_TAXI_OUT.TAXI_OUT)), (COALESCE(tmp_flights_DEP_DELAY.TAXI_IN, tmp_imputed_TAXI_IN.TAXI_IN)), (COALESCE(tmp_flights_DEP_DELAY.DIVERTED, tmp_imputed_DIVERTED.DIVERTED)), (COALESCE(tmp_flights_DEP_DELAY.ARR_DELAY, tmp_imputed_ARR_DELAY.ARR_DELAY)), tmp_imputed_DEP_DELAY.DEP_DELAY, (ACTUAL_ELAPSED_TIME), (AIR_TIME), (DEP_TIME_HOUR), (DEP_TIME_MIN), (WHEELS_OFF_HOUR), (WHEELS_OFF_MIN), (WHEELS_ON_HOUR), (WHEELS_ON_MIN), (ARR_TIME_HOUR), (ARR_TIME_MIN), (MONTH_SIN), (MONTH_COS), (DAY_SIN), (DAY_COS), (WEEKDAY_SIN), (WEEKDAY_COS), (EXTRA_DAY_ARR), (EXTRA_DAY_DEP)], ARRAY[]::int[]))
FROM
tmp_flights_DEP_DELAY JOIN tmp_imputed_DEP_DELAY ON tmp_flights_DEP_DELAY.id = tmp_imputed_DEP_DELAY.id
JOIN flight.schedule ON tmp_flights_DEP_DELAY.flight = flight.schedule.flight
LEFT OUTER JOIN tmp_imputed_CRS_DEP_HOUR ON tmp_flights_DEP_DELAY.flight = tmp_imputed_CRS_DEP_HOUR.flight
JOIN flight.distances ON flight.distances.airports = flight.schedule.airports
LEFT OUTER JOIN tmp_imputed_TAXI_OUT ON tmp_flights_DEP_DELAY.id = tmp_imputed_TAXI_OUT.id
LEFT OUTER JOIN tmp_imputed_TAXI_IN ON tmp_flights_DEP_DELAY.id = tmp_imputed_TAXI_IN.id
LEFT OUTER JOIN tmp_imputed_DIVERTED ON tmp_flights_DEP_DELAY.id = tmp_imputed_DIVERTED.id
LEFT OUTER JOIN tmp_imputed_DISTANCE ON flight.distances.airports = tmp_imputed_DISTANCE.airports
LEFT OUTER JOIN tmp_imputed_ARR_DELAY ON tmp_flights_DEP_DELAY.id = tmp_imputed_ARR_DELAY.id';
                _start_ts := clock_timestamp();
                execute(query_cof_diff) INTO _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE ' SELECT DONE %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
                _triple_train := _triple - _triple_removal;

                _label_idx := 9;
                _params := ridge_linear_regression(_triple_train, _label_idx, 0.001, 1, 1000);

                EXECUTE 'DROP INDEX idx_imputed_DEP_DELAY;';
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
                EXECUTE 'create index idx_imputed_DEP_DELAY ON tmp_imputed_DEP_DELAY USING btree (id)';
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


CREATE OR REPLACE FUNCTION test_lda() RETURNS void AS $$
  DECLARE
    _triple cofactor;
      _params float8[];
  BEGIN

    EXECUTE 'select sum(to_cofactor(ARRAY[a,b,c], ARRAY[a,b,c])) from test2' into _triple;
    _params := lda_train(_triple, 1);
  END;
$$ LANGUAGE plpgsql;