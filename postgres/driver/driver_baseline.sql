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

    --RAISE NOTICE '%', format('CREATE INDEX idx_tmp_id_%s_%s ON tmp_%s_%s USING btree (%s)', VARIADIC ARRAY[tbl.name, col_nan] || tbl.name || col_nan || tbl.identifier);
    --EXECUTE format('CREATE INDEX idx_tmp_id_%s_%s ON tmp_%s_%s USING btree (%s)', VARIADIC ARRAY[tbl.name, col_nan] || tbl.name || col_nan || tbl.identifier);
    IF cardinality(tbl.join_keys) > 0 then
    --    RAISE NOTICE '%', format('CREATE INDEX idx_tmp_%s_%s ON tmp_%s_%s USING btree (%s)', VARIADIC ARRAY[tbl.name, col_nan] || tbl.name || col_nan || tbl.join_keys[1]);
    --    EXECUTE format('CREATE INDEX idx_tmp_%s_%s ON tmp_%s_%s USING btree (%s)', VARIADIC ARRAY[tbl.name, col_nan] || tbl.name || col_nan || tbl.join_keys[1]);
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

    --RAISE NOTICE 'create index idx_imputed_% ON tmp_imputed_% USING btree (%)', col_nan, col_nan, tbl.identifier[1];
    --EXECUTE format('create index idx_imputed_%s ON tmp_imputed_%s USING btree (%s)', VARIADIC ARRAY[col_nan, col_nan, tbl.identifier[1]]);
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
      queries_col_only text[] := ARRAY[]::text[];
      query text;
    query2 text;
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

      EXECUTE 'DROP TABLE IF EXISTS join_table';

    FOREACH tbl IN ARRAY tables
    LOOP
        _imputed_data := standard_imputation(tbl);
        queries[index] := '';
        queries_col_only[index] := '';
        FOR _counter in 1..cardinality(tbl.columns_null)
        LOOP
            --perform create_table_missing_values(tbl,  tbl.columns_null[_counter]);
            --perform create_table_imputed_values(tbl, tbl.columns_null[_counter], _imputed_data[_counter]);
            queries[index] := queries[index] || format('COALESCE(%s, %s), ', tbl.columns_null[_counter], _imputed_data[_counter]);
            queries_col_only[index] := queries_col_only[index] || format('%s, ', tbl.columns_null[_counter]);
        end loop;
        FOREACH col IN ARRAY tbl.columns_no_null
        LOOP
            queries[index] := queries[index] || col || ' , ';
            queries_col_only[index] := queries_col_only[index] || col || ' , ';
        end loop;
        RAISE NOTICE 'Table LOOP: %', queries[index];
        index = index + 1;

    end loop;

    RAISE NOTICE '%', format('SELECT SUM(to_cofactor(ARRAY[%s], ARRAY[]::int[])) FROM join_table', RTRIM(queries[1] || queries[2] || queries[3],  ', '));

    query := 'CREATE TEMPORARY TABLE join_table AS (SELECT * FROM flight.Route JOIN flight.schedule USING (ROUTE_ID) JOIN flight.flight USING (SCHEDULE_ID))';
    RAISE NOTICE '%', query;
    _start_ts := clock_timestamp();
    EXECUTE query;
    _end_ts := clock_timestamp();
    RAISE NOTICE 'Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
    _start_ts := clock_timestamp();
    --RAISE NOTICE 'ALTER TABLE join_table SET (parallel_workers = 20)';
    --EXECUTE 'ALTER TABLE join_table SET (parallel_workers = 20)';
    _end_ts := clock_timestamp();
    RAISE NOTICE 'Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));

    query := 'ALTER TABLE join_table ';
    query2 := 'UPDATE join_table SET ';

    FOREACH tbl IN ARRAY tables
    LOOP
        FOR _counter in 1..cardinality(tbl.columns_null)
        LOOP
            _start_ts := clock_timestamp();
            query := query || format('ADD COLUMN is_null_%s BOOL , ', tbl.columns_null[_counter]);
            query2 := query2 || format(' is_null_%s = %s IS NULL , ', tbl.columns_null[_counter], tbl.columns_null[_counter]);
        end loop;
    END LOOP;

    query := RTRIM(query,  ', ');
    query2 := RTRIM(query2,  ', ');
    RAISE NOTICE '%', query;
    _start_ts := clock_timestamp();
    EXECUTE query;
    _end_ts := clock_timestamp();
    RAISE NOTICE 'Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
    RAISE NOTICE '%', query2;
    _start_ts := clock_timestamp();
    EXECUTE query2;
    _end_ts := clock_timestamp();

    EXECUTE 'CREATE INDEX idx_is_null_distance ON join_table (is_null_distance) WHERE is_null_distance IS TRUE';
    EXECUTE 'CREATE INDEX idx_is_null_CRS_DEP_HOUR ON join_table (is_null_CRS_DEP_HOUR) WHERE is_null_CRS_DEP_HOUR IS TRUE';
    EXECUTE 'CREATE INDEX idx_is_null_TAXI_OUT ON join_table (is_null_TAXI_OUT) WHERE is_null_TAXI_OUT IS TRUE';
    EXECUTE 'CREATE INDEX idx_is_null_TAXI_IN ON join_table (is_null_TAXI_IN) WHERE is_null_TAXI_IN IS TRUE';
    EXECUTE 'CREATE INDEX idx_is_null_DIVERTED ON join_table (is_null_DIVERTED) WHERE is_null_DIVERTED IS TRUE';
    EXECUTE 'CREATE INDEX idx_is_null_ARR_DELAY ON join_table (is_null_ARR_DELAY) WHERE is_null_ARR_DELAY IS TRUE';
    EXECUTE 'CREATE INDEX idx_is_null_DEP_DELAY ON join_table (is_null_DEP_DELAY) WHERE is_null_DEP_DELAY IS TRUE';

    EXECUTE 'CREATE INDEX idx_ROUTE_ID ON join_table (ROUTE_ID)';
    EXECUTE 'CREATE INDEX idx_SCHEDULE_ID ON join_table (SCHEDULE_ID)';

    RAISE NOTICE 'Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));

    query := 'UPDATE join_table SET ';

    ---default imputation
    FOREACH tbl IN ARRAY tables
    LOOP
        _imputed_data := standard_imputation(tbl);
        FOR _counter in 1..cardinality(tbl.columns_null)
        LOOP
            query := query || format(' %s = COALESCE(%s, %s), ', tbl.columns_null[_counter], tbl.columns_null[_counter], _imputed_data[_counter]);
        end loop;
    END LOOP;

    query := RTRIM(query,  ', ');
    RAISE NOTICE '%', query;
    _start_ts := clock_timestamp();
    EXECUTE query;
    _end_ts := clock_timestamp();
    RAISE NOTICE 'Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));


    RAISE NOTICE '%', format('SELECT SUM(to_cofactor(ARRAY[%s], ARRAY[]::int[])) FROM join_table', RTRIM(queries[1] || queries[2] || queries[3],  ', '));
    query := format('SELECT SUM(to_cofactor(ARRAY[%s], ARRAY[]::int[])) FROM join_table', RTRIM(queries[1] || queries[2] || queries[3],  ', '));
    _start_ts := clock_timestamp();
    EXECUTE query into _triple;
    _end_ts := clock_timestamp();
    RAISE NOTICE 'Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));


    for counter in 1..iterations
    loop
                RAISE NOTICE 'Imputing distance';
                ---WHERE is_null_%s IS TRUE
                query_cof_diff := format('SELECT SUM(to_cofactor(ARRAY[%s], ARRAY[]::int[])) FROM join_table WHERE is_null_%s IS FALSE', RTRIM(queries_col_only[1] || queries_col_only[2] || queries_col_only[3],  ', '), 'DISTANCE');
                _start_ts := clock_timestamp();
                execute(query_cof_diff) INTO _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Generate cofactor of nan values: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
                ---_triple_train := _triple - _triple_removal;
                _label_idx := 0;--array_position(_columns, col)-1;--c code
                _start_ts := clock_timestamp();
                _params := ridge_linear_regression(_triple_removal, _label_idx, 0.001, 0, 1000);
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Train the model: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
                --update cofactor matrix and table
                _start_ts := clock_timestamp();
                EXECUTE 'CREATE TEMPORARY TABLE tmp_imputed AS (SELECT ROUTE_ID, AVG(('||_params[1]::text || ')+(' || _params[3]::text||'*CRS_DEP_HOUR)+('||_params[4]::text||'*CRS_DEP_MIN)+('||_params[5]::text||'*CRS_ARR_HOUR)+('||_params[6]::text||'*CRS_ARR_MIN)+('||_params[7]::text||'*TAXI_OUT)+('||_params[8]::text||'*TAXI_IN)+' ||
                        '('||_params[9]::text||'*DIVERTED)+('||_params[10]::text||'*ARR_DELAY)+('||_params[11]::text||'*DEP_DELAY)+('||_params[12]::text||'*ACTUAL_ELAPSED_TIME) + ('||_params[13]::text||'*AIR_TIME) + ('||_params[14]::text||'*DEP_TIME_HOUR) + ('||_params[15]::text||'*DEP_TIME_MIN) + ('||_params[16]::text||'*WHEELS_OFF_HOUR) + ('||_params[17]::text||'*WHEELS_OFF_MIN) + ('||_params[18]::text||'*WHEELS_ON_HOUR) + ('||_params[19]::text||'*WHEELS_ON_MIN) + ('||_params[20]::text||'*ARR_TIME_HOUR) + ('||_params[21]::text||'*ARR_TIME_MIN) + ('||_params[22]::text||'*MONTH_SIN) + ('||_params[23]::text||'*MONTH_COS) + ('||_params[24]::text||'*DAY_SIN) + ('||_params[25]::text||'*DAY_COS) + ('||_params[26]::text||'*WEEKDAY_SIN) + ('||_params[27]::text||'*WEEKDAY_COS) + ('||_params[28]::text||'*EXTRA_DAY_ARR) + ('||_params[29]::text||'*EXTRA_DAY_DEP))' ||
                        ' AS value FROM join_table WHERE is_null_DISTANCE IS TRUE GROUP BY join_table.route_id)';

                --RAISE NOTICE 'Query: % ' , query;
                EXECUTE query;
                EXECUTE 'UPDATE join_table SET DISTANCE = tmp_imputed.value FROM tmp_imputed WHERE tmp_imputed.ROUTE_ID = join_table.ROUTE_ID';
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Impute new values and update cofactor matrix: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));

                _start_ts := clock_timestamp();
                ---EXECUTE query_cof_diff INTO _triple_removal;
                ---_triple := _triple_train + _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Impute new values and update cofactor matrix: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
                EXECUTE 'DROP TABLE tmp_imputed';

                RAISE NOTICE 'Imputing CRS_DEP_HOUR';

                query_cof_diff := format('SELECT SUM(to_cofactor(ARRAY[%s], ARRAY[]::int[])) FROM join_table WHERE is_null_%s IS FALSE', RTRIM(queries_col_only[1] || queries_col_only[2] || queries_col_only[3],  ', '), 'CRS_DEP_HOUR');
                _start_ts := clock_timestamp();
                execute(query_cof_diff) INTO _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Generate cofactor of nan values: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
                ---_triple_train := _triple - _triple_removal;
                _label_idx := 1;--array_position(_columns, col)-1;--c code
                _start_ts := clock_timestamp();
                _params := ridge_linear_regression(_triple_removal, _label_idx, 0.001, 0, 1000);
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Train the model: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
                --update cofactor matrix and table
                _start_ts := clock_timestamp();
                EXECUTE 'CREATE TEMPORARY TABLE tmp_imputed AS (SELECT SCHEDULE_ID, AVG(('||_params[1]::text || ')+(' || _params[2]::text||'*DISTANCE)+('||_params[4]::text||'*CRS_DEP_MIN)+('||_params[5]::text||'*CRS_ARR_HOUR)+('||_params[6]::text||'*CRS_ARR_MIN)+('||_params[7]::text||'*TAXI_OUT)+('||_params[8]::text||'*TAXI_IN)+' ||
                        '('||_params[9]::text||'*DIVERTED)+('||_params[10]::text||'*ARR_DELAY)+('||_params[11]::text||'*DEP_DELAY)+('||_params[12]::text||'*ACTUAL_ELAPSED_TIME) + ('||_params[13]::text||'*AIR_TIME) + ('||_params[14]::text||'*DEP_TIME_HOUR) + ('||_params[15]::text||'*DEP_TIME_MIN) + ('||_params[16]::text||'*WHEELS_OFF_HOUR) + ('||_params[17]::text||'*WHEELS_OFF_MIN) + ('||_params[18]::text||'*WHEELS_ON_HOUR) + ('||_params[19]::text||'*WHEELS_ON_MIN) + ('||_params[20]::text||'*ARR_TIME_HOUR) + ('||_params[21]::text||'*ARR_TIME_MIN) + ('||_params[22]::text||'*MONTH_SIN) + ('||_params[23]::text||'*MONTH_COS) + ('||_params[24]::text||'*DAY_SIN) + ('||_params[25]::text||'*DAY_COS) + ('||_params[26]::text||'*WEEKDAY_SIN) + ('||_params[27]::text||'*WEEKDAY_COS) + ('||_params[28]::text||'*EXTRA_DAY_ARR) + ('||_params[29]::text||'*EXTRA_DAY_DEP))' ||
                        ' AS value FROM join_table WHERE is_null_DISTANCE IS TRUE GROUP BY join_table.SCHEDULE_ID)';

                --RAISE NOTICE 'Query: % ' , query;
                EXECUTE query;
                EXECUTE 'UPDATE join_table SET CRS_DEP_HOUR = tmp_imputed.value FROM tmp_imputed WHERE tmp_imputed.SCHEDULE_ID = join_table.SCHEDULE_ID';
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Impute new values and update cofactor matrix: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));

                _start_ts := clock_timestamp();
                --EXECUTE query_cof_diff INTO _triple_removal;
                --_triple := _triple_train + _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Impute new values and update cofactor matrix: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
                EXECUTE 'DROP TABLE tmp_imputed';


                RAISE NOTICE 'Imputing TAXI_OUT';

                query_cof_diff := format('SELECT SUM(to_cofactor(ARRAY[%s], ARRAY[]::int[])) FROM join_table WHERE is_null_%s IS FALSE', RTRIM(queries_col_only[1] || queries_col_only[2] || queries_col_only[3],  ', '), 'TAXI_OUT');
                _start_ts := clock_timestamp();
                execute(query_cof_diff) INTO _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Generate cofactor of nan values: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
                --_triple_train := _triple - _triple_removal;
                _label_idx := 5;--array_position(_columns, col)-1;--c code
                _start_ts := clock_timestamp();
                _params := ridge_linear_regression(_triple_removal, _label_idx, 0.001, 0, 1000);
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Train the model: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
                --update cofactor matrix and table
                _start_ts := clock_timestamp();
                EXECUTE 'CREATE TEMPORARY TABLE tmp_imputed AS (SELECT id, (('||_params[1]::text || ')+(' || _params[2]::text||'*DISTANCE)+('||_params[3]::text||'*CRS_DEP_HOUR)+('||_params[4]::text||'*CRS_DEP_MIN)+('||_params[5]::text||'*CRS_ARR_HOUR)+('||_params[6]::text||'*CRS_ARR_MIN)+('||_params[8]::text||'*TAXI_IN)+' ||
                        '('||_params[9]::text||'*DIVERTED)+('||_params[10]::text||'*ARR_DELAY)+('||_params[11]::text||'*DEP_DELAY)+('||_params[12]::text||'*ACTUAL_ELAPSED_TIME) + ('||_params[13]::text||'*AIR_TIME) + ('||_params[14]::text||'*DEP_TIME_HOUR) + ('||_params[15]::text||'*DEP_TIME_MIN) + ('||_params[16]::text||'*WHEELS_OFF_HOUR) + ('||_params[17]::text||'*WHEELS_OFF_MIN) + ('||_params[18]::text||'*WHEELS_ON_HOUR) + ('||_params[19]::text||'*WHEELS_ON_MIN) + ('||_params[20]::text||'*ARR_TIME_HOUR) + ('||_params[21]::text||'*ARR_TIME_MIN) + ('||_params[22]::text||'*MONTH_SIN) + ('||_params[23]::text||'*MONTH_COS) + ('||_params[24]::text||'*DAY_SIN) + ('||_params[25]::text||'*DAY_COS) + ('||_params[26]::text||'*WEEKDAY_SIN) + ('||_params[27]::text||'*WEEKDAY_COS) + ('||_params[28]::text||'*EXTRA_DAY_ARR) + ('||_params[29]::text||'*EXTRA_DAY_DEP))' ||
                        ' AS value FROM join_table WHERE is_null_TAXI_OUT IS TRUE)';

                --RAISE NOTICE 'Query: % ' , query;
                EXECUTE query;
                EXECUTE 'UPDATE join_table SET TAXI_OUT = tmp_imputed.value FROM tmp_imputed WHERE tmp_imputed.id = join_table.id';
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Impute new values and update cofactor matrix: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));

                _start_ts := clock_timestamp();
                --EXECUTE query_cof_diff INTO _triple_removal;
                --_triple := _triple_train + _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Impute new values and update cofactor matrix: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
                EXECUTE 'DROP TABLE tmp_imputed';

                ------

                RAISE NOTICE 'Imputing TAXI_IN';

                query_cof_diff := format('SELECT SUM(to_cofactor(ARRAY[%s], ARRAY[]::int[])) FROM join_table WHERE is_null_%s IS FALSE', RTRIM(queries_col_only[1] || queries_col_only[2] || queries_col_only[3],  ', '), 'TAXI_IN');
                _start_ts := clock_timestamp();
                execute(query_cof_diff) INTO _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Generate cofactor of nan values: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
                --_triple_train := _triple - _triple_removal;
                _label_idx := 6;--array_position(_columns, col)-1;--c code
                _start_ts := clock_timestamp();
                _params := ridge_linear_regression(_triple_removal, _label_idx, 0.001, 0, 1000);
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Train the model: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
                --update cofactor matrix and table
                _start_ts := clock_timestamp();
                EXECUTE 'CREATE TEMPORARY TABLE tmp_imputed AS (SELECT id, (('||_params[1]::text || ')+(' || _params[2]::text||'*DISTANCE)+('||_params[3]::text||'*CRS_DEP_HOUR)+('||_params[4]::text||'*CRS_DEP_MIN)+('||_params[5]::text||'*CRS_ARR_HOUR)+('||_params[6]::text||'*CRS_ARR_MIN)+('||_params[7]::text||'*TAXI_OUT)+' ||
                        '('||_params[9]::text||'*DIVERTED)+('||_params[10]::text||'*ARR_DELAY)+('||_params[11]::text||'*DEP_DELAY)+('||_params[12]::text||'*ACTUAL_ELAPSED_TIME) + ('||_params[13]::text||'*AIR_TIME) + ('||_params[14]::text||'*DEP_TIME_HOUR) + ('||_params[15]::text||'*DEP_TIME_MIN) + ('||_params[16]::text||'*WHEELS_OFF_HOUR) + ('||_params[17]::text||'*WHEELS_OFF_MIN) + ('||_params[18]::text||'*WHEELS_ON_HOUR) + ('||_params[19]::text||'*WHEELS_ON_MIN) + ('||_params[20]::text||'*ARR_TIME_HOUR) + ('||_params[21]::text||'*ARR_TIME_MIN) + ('||_params[22]::text||'*MONTH_SIN) + ('||_params[23]::text||'*MONTH_COS) + ('||_params[24]::text||'*DAY_SIN) + ('||_params[25]::text||'*DAY_COS) + ('||_params[26]::text||'*WEEKDAY_SIN) + ('||_params[27]::text||'*WEEKDAY_COS) + ('||_params[28]::text||'*EXTRA_DAY_ARR) + ('||_params[29]::text||'*EXTRA_DAY_DEP))' ||
                        ' AS value FROM join_table WHERE is_null_TAXI_IN IS TRUE)';

                --RAISE NOTICE 'Query: % ' , query;
                EXECUTE query;
                EXECUTE 'UPDATE join_table SET TAXI_IN = tmp_imputed.value FROM tmp_imputed WHERE tmp_imputed.id = join_table.id';
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Impute new values and update cofactor matrix: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));

                _start_ts := clock_timestamp();
                --EXECUTE query_cof_diff INTO _triple_removal;
                --_triple := _triple_train + _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Impute new values and update cofactor matrix: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
                EXECUTE 'DROP TABLE tmp_imputed';

                ------

                RAISE NOTICE 'Imputing DIVERTED';

                query_cof_diff := format('SELECT SUM(to_cofactor(ARRAY[%s], ARRAY[]::int[])) FROM join_table WHERE is_null_%s IS FALSE', RTRIM(queries_col_only[1] || queries_col_only[2] || queries_col_only[3],  ', '), 'DIVERTED');
                _start_ts := clock_timestamp();
                execute(query_cof_diff) INTO _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Generate cofactor of nan values: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
                --_triple_train := _triple - _triple_removal;
                _label_idx := 7;--array_position(_columns, col)-1;--c code
                _start_ts := clock_timestamp();
                _params := ridge_linear_regression(_triple_removal, _label_idx, 0.001, 0, 1000);
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Train the model: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
                --update cofactor matrix and table
                _start_ts := clock_timestamp();
                EXECUTE 'CREATE TEMPORARY TABLE tmp_imputed AS (SELECT id, (('||_params[1]::text || ')+(' || _params[2]::text||'*DISTANCE)+('||_params[3]::text||'*CRS_DEP_HOUR)+('||_params[4]::text||'*CRS_DEP_MIN)+('||_params[5]::text||'*CRS_ARR_HOUR)+('||_params[6]::text||'*CRS_ARR_MIN)+('||_params[7]::text||'*TAXI_OUT)+' ||
                        '('||_params[8]::text||'*TAXI_IN)+('||_params[10]::text||'*ARR_DELAY)+('||_params[11]::text||'*DEP_DELAY)+('||_params[12]::text||'*ACTUAL_ELAPSED_TIME) + ('||_params[13]::text||'*AIR_TIME) + ('||_params[14]::text||'*DEP_TIME_HOUR) + ('||_params[15]::text||'*DEP_TIME_MIN) + ('||_params[16]::text||'*WHEELS_OFF_HOUR) + ('||_params[17]::text||'*WHEELS_OFF_MIN) + ('||_params[18]::text||'*WHEELS_ON_HOUR) + ('||_params[19]::text||'*WHEELS_ON_MIN) + ('||_params[20]::text||'*ARR_TIME_HOUR) + ('||_params[21]::text||'*ARR_TIME_MIN) + ('||_params[22]::text||'*MONTH_SIN) + ('||_params[23]::text||'*MONTH_COS) + ('||_params[24]::text||'*DAY_SIN) + ('||_params[25]::text||'*DAY_COS) + ('||_params[26]::text||'*WEEKDAY_SIN) + ('||_params[27]::text||'*WEEKDAY_COS) + ('||_params[28]::text||'*EXTRA_DAY_ARR) + ('||_params[29]::text||'*EXTRA_DAY_DEP))' ||
                        ' AS value FROM join_table WHERE is_null_DIVERTED IS TRUE)';

                --RAISE NOTICE 'Query: % ' , query;
                EXECUTE query;
                EXECUTE 'UPDATE join_table SET DIVERTED = tmp_imputed.value FROM tmp_imputed WHERE tmp_imputed.id = join_table.id';
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Impute new values and update cofactor matrix: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));

                _start_ts := clock_timestamp();
                --EXECUTE query_cof_diff INTO _triple_removal;
                --_triple := _triple_train + _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Impute new values and update cofactor matrix: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
                EXECUTE 'DROP TABLE tmp_imputed';

                ------

                RAISE NOTICE 'Imputing ARR_DELAY';

                query_cof_diff := format('SELECT SUM(to_cofactor(ARRAY[%s], ARRAY[]::int[])) FROM join_table WHERE is_null_%s IS FALSE', RTRIM(queries_col_only[1] || queries_col_only[2] || queries_col_only[3],  ', '), 'ARR_DELAY');
                _start_ts := clock_timestamp();
                execute(query_cof_diff) INTO _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Generate cofactor of nan values: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
                --_triple_train := _triple - _triple_removal;
                _label_idx := 8;--array_position(_columns, col)-1;--c code
                _start_ts := clock_timestamp();
                _params := ridge_linear_regression(_triple_removal, _label_idx, 0.001, 0, 1000);
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Train the model: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
                --update cofactor matrix and table
                _start_ts := clock_timestamp();
                EXECUTE 'CREATE TEMPORARY TABLE tmp_imputed AS (SELECT id, (('||_params[1]::text || ')+(' || _params[2]::text||'*DISTANCE)+('||_params[3]::text||'*CRS_DEP_HOUR)+('||_params[4]::text||'*CRS_DEP_MIN)+('||_params[5]::text||'*CRS_ARR_HOUR)+('||_params[6]::text||'*CRS_ARR_MIN)+('||_params[7]::text||'*TAXI_OUT)+' ||
                        '('||_params[8]::text||'*TAXI_IN)+('||_params[9]::text||'*DIVERTED)+('||_params[11]::text||'*DEP_DELAY)+('||_params[12]::text||'*ACTUAL_ELAPSED_TIME) + ('||_params[13]::text||'*AIR_TIME) + ('||_params[14]::text||'*DEP_TIME_HOUR) + ('||_params[15]::text||'*DEP_TIME_MIN) + ('||_params[16]::text||'*WHEELS_OFF_HOUR) + ('||_params[17]::text||'*WHEELS_OFF_MIN) + ('||_params[18]::text||'*WHEELS_ON_HOUR) + ('||_params[19]::text||'*WHEELS_ON_MIN) + ('||_params[20]::text||'*ARR_TIME_HOUR) + ('||_params[21]::text||'*ARR_TIME_MIN) + ('||_params[22]::text||'*MONTH_SIN) + ('||_params[23]::text||'*MONTH_COS) + ('||_params[24]::text||'*DAY_SIN) + ('||_params[25]::text||'*DAY_COS) + ('||_params[26]::text||'*WEEKDAY_SIN) + ('||_params[27]::text||'*WEEKDAY_COS) + ('||_params[28]::text||'*EXTRA_DAY_ARR) + ('||_params[29]::text||'*EXTRA_DAY_DEP))' ||
                        ' AS value FROM join_table WHERE is_null_DIVERTED IS TRUE)';

                --RAISE NOTICE 'Query: % ' , query;
                EXECUTE query;
                EXECUTE 'UPDATE join_table SET ARR_DELAY = tmp_imputed.value FROM tmp_imputed WHERE tmp_imputed.id = join_table.id';
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Impute new values and update cofactor matrix: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));

                _start_ts := clock_timestamp();
                --EXECUTE query_cof_diff INTO _triple_removal;
                --_triple := _triple_train + _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Impute new values and update cofactor matrix: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
                EXECUTE 'DROP TABLE tmp_imputed';


                ------

                RAISE NOTICE 'Imputing DEP_DELAY';

                query_cof_diff := format('SELECT SUM(to_cofactor(ARRAY[%s], ARRAY[]::int[])) FROM join_table WHERE is_null_%s IS FALSE', RTRIM(queries_col_only[1] || queries_col_only[2] || queries_col_only[3],  ', '), 'DEP_DELAY');
                _start_ts := clock_timestamp();
                execute(query_cof_diff) INTO _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Generate cofactor of nan values: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
                --_triple_train := _triple - _triple_removal;
                _label_idx := 9;--array_position(_columns, col)-1;--c code
                _start_ts := clock_timestamp();
                _params := ridge_linear_regression(_triple_removal, _label_idx, 0.001, 0, 1000);
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Train the model: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
                --update cofactor matrix and table
                _start_ts := clock_timestamp();
                EXECUTE 'CREATE TEMPORARY TABLE tmp_imputed AS (SELECT id, (('||_params[1]::text || ')+(' || _params[2]::text||'*DISTANCE)+('||_params[3]::text||'*CRS_DEP_HOUR)+('||_params[4]::text||'*CRS_DEP_MIN)+('||_params[5]::text||'*CRS_ARR_HOUR)+('||_params[6]::text||'*CRS_ARR_MIN)+('||_params[7]::text||'*TAXI_OUT)+' ||
                        '('||_params[8]::text||'*TAXI_IN)+('||_params[9]::text||'*DIVERTED)+('||_params[10]::text||'*ARR_DELAY)+('||_params[12]::text||'*ACTUAL_ELAPSED_TIME) + ('||_params[13]::text||'*AIR_TIME) + ('||_params[14]::text||'*DEP_TIME_HOUR) + ('||_params[15]::text||'*DEP_TIME_MIN) + ('||_params[16]::text||'*WHEELS_OFF_HOUR) + ('||_params[17]::text||'*WHEELS_OFF_MIN) + ('||_params[18]::text||'*WHEELS_ON_HOUR) + ('||_params[19]::text||'*WHEELS_ON_MIN) + ('||_params[20]::text||'*ARR_TIME_HOUR) + ('||_params[21]::text||'*ARR_TIME_MIN) + ('||_params[22]::text||'*MONTH_SIN) + ('||_params[23]::text||'*MONTH_COS) + ('||_params[24]::text||'*DAY_SIN) + ('||_params[25]::text||'*DAY_COS) + ('||_params[26]::text||'*WEEKDAY_SIN) + ('||_params[27]::text||'*WEEKDAY_COS) + ('||_params[28]::text||'*EXTRA_DAY_ARR) + ('||_params[29]::text||'*EXTRA_DAY_DEP))' ||
                        ' AS value FROM join_table WHERE is_null_DIVERTED IS TRUE)';

                --RAISE NOTICE 'Query: % ' , query;
                EXECUTE query;
                EXECUTE 'UPDATE join_table SET DEP_DELAY = tmp_imputed.value FROM tmp_imputed WHERE tmp_imputed.id = join_table.id';
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Impute new values and update cofactor matrix: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));

                _start_ts := clock_timestamp();
                --EXECUTE query_cof_diff INTO _triple_removal;
                --_triple := _triple_train + _triple_removal;
                _end_ts   := clock_timestamp();
                RAISE NOTICE 'Impute new values and update cofactor matrix: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
                EXECUTE 'DROP TABLE tmp_imputed';

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
    tbls := tbls || row('Route', 'flight', '{}', '{"DISTANCE"}', '{"ROUTE_ID"}', '{}')::t_info;
    tbls := tbls || row('schedule', 'flight', '{"CRS_DEP_MIN","CRS_ARR_HOUR","CRS_ARR_MIN"}', '{"CRS_DEP_HOUR"}', '{"SCHEDULE_ID"}', '{"ROUTE_ID"}')::t_info;
    tbls := tbls || row('flight', 'flight', '{"ACTUAL_ELAPSED_TIME","AIR_TIME","DEP_TIME_HOUR","DEP_TIME_MIN","WHEELS_OFF_HOUR","WHEELS_OFF_MIN","WHEELS_ON_HOUR","WHEELS_ON_MIN","ARR_TIME_HOUR","ARR_TIME_MIN","MONTH_SIN","MONTH_COS","DAY_SIN","DAY_COS","WEEKDAY_SIN","WEEKDAY_COS","EXTRA_DAY_ARR","EXTRA_DAY_DEP"}', '{"TAXI_OUT","TAXI_IN", "DIVERTED", "ARR_DELAY","DEP_DELAY"}', '{"id"}', '{"SCHEDULE_ID"}')::t_info;
    join_tables := ARRAY['Inventory.locn = Location.locn', 'Inventory.locn = Weather.locn AND Inventory.dateid = Weather.dateid', 'Inventory.ksn = Retailer.ksn', 'Census.zip = Location.zip'];

    return impute(tbls,2);
  END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION test_lda() RETURNS void AS $$
  DECLARE
    _triple cofactor;
    val float4;
      _params float4[];
    label int;
  BEGIN
    EXECUTE 'select sum(to_cofactor(ARRAY[a,b,c], ARRAY[a,b,c])) from test2' into _triple;
    _params := lda_train(_triple, 1);
    FOREACH val IN ARRAY _params
    LOOP
        RAISE NOTICE '%', val;
    end LOOP;
    EXECUTE format('SELECT lda_impute(ARRAY[%s], ARRAY[a,b,c]) from test2', array_to_string(_params, ',')) into label;
    ---make sure cofactor order of columns is respected
  END;
$$ LANGUAGE plpgsql;