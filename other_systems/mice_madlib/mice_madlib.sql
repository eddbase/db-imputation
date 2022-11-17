CREATE OR REPLACE FUNCTION standard_imputation(columns_null_numeric text[], columns_null_cat text[]) RETURNS float8[] AS $$
  DECLARE
    col text;
    columns text[];
    avg_imputations float8[];
    query text := 'SELECT ARRAY[';
  BEGIN
    FOREACH col IN ARRAY columns_null_numeric
    LOOP
        query := query || 'AVG(CASE WHEN %s IS NOT NULL THEN %s END), ';
        columns := columns || col || col;
    end LOOP;

    FOREACH col IN ARRAY columns_null_cat
    LOOP
        query := query || 'mode() WITHIN GROUP (ORDER BY CASE WHEN %s IS NOT NULL THEN %s END), ';
        columns := columns || col || col;
    end LOOP;

    query := RTRIM(query, ', ') || '] FROM tmp_table;';

    RAISE NOTICE '%', format(query, VARIADIC columns);

    EXECUTE format(query, VARIADIC columns) INTO avg_imputations;
   RETURN avg_imputations;
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION create_table_missing_values(columns text[], col_nan text) RETURNS void AS $$
  DECLARE
    col text;
    columns_no_null text[];
    insert_query text := 'CREATE TEMPORARY TABLE tmp_min%s AS SELECT ';
  BEGIN
    columns_no_null := array_remove(columns,  col_nan);
    FOREACH col IN ARRAY columns_no_null
    LOOP
        insert_query := insert_query || '%s, ';
    end loop;
    insert_query := RTRIM(insert_query, ', ') || ' FROM join_result WHERE %s IS NULL';
    EXECUTE format('DROP TABLE IF EXISTS tmp_min%s', col_nan);
    RAISE NOTICE 'create_table_missing_values:  %', format(insert_query, VARIADIC ARRAY[col_nan] || columns_no_null || col_nan);
    EXECUTE format(insert_query, VARIADIC ARRAY[col_nan] || columns_no_null || col_nan);
  END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION create_table_imputed_values(col_nan text, average float8) RETURNS void AS $$
  DECLARE
    insert_query text := 'CREATE TEMPORARY TABLE tmp_imputed_%s AS SELECT flight, ';
  BEGIN
    insert_query := insert_query || '%s AS %s FROM tmp_min%s';
    EXECUTE format('DROP TABLE IF EXISTS tmp_imputed_%s', col_nan);
    RAISE NOTICE 'create_table_imputed_values:  %', format(insert_query, VARIADIC ARRAY[col_nan] || average::text || col_nan, col_nan);
    EXECUTE format(insert_query, VARIADIC ARRAY[col_nan] || average::text || col_nan, col_nan);
  END;
$$ LANGUAGE plpgsql;

create or replace function array_diff(array1 anyarray, array2 anyarray)
returns anyarray language sql immutable as $$
    select coalesce(array_agg(elem), '{}')
    from unnest(array1) elem
    where elem <> all(array2)
$$;

--skip_null: treats a null as %s

CREATE OR REPLACE FUNCTION main() RETURNS int AS $$
    DECLARE
        num_columns_null text[] := ARRAY['CRS_DEP_HOUR', 'DISTANCE', 'TAXI_OUT', 'TAXI_IN', 'ARR_DELAY', 'DEP_DELAY'];
        cat_columns_null text[] := ARRAY['DIVERTED'];
        columns_null text[] := num_columns_null || cat_columns_null;
        columns text[] := ARRAY['id', 'airports', 'DISTANCE', 'flight', 'CRS_DEP_HOUR', 'CRS_DEP_MIN', 'CRS_ARR_HOUR', 'CRS_ARR_MIN'];
        _imputed_data float8[];
        mice_iter int := 7;
        _counter int;
        _counter_2 int;
        _query text := 'SELECT ';
        _query_2 text := '';
        columns_no_null text[];
        params text[];
        _indep_vars text[];
        col text;
        _start_ts timestamptz;
    	_end_ts   timestamptz;
		_int_ts timestamptz;


    BEGIN
--OP_CARRIER
--
        --EXECUTE 'SELECT madlib.encode_categorical_variables (''flight.distances'', ''flight.distances_cat'', ''ORIGIN, DEST'', NULL, ''airports'')';
        --EXECUTE 'SELECT madlib.encode_categorical_variables (''flight.schedule'', ''flight.schedule_cat'', ''OP_CARRIER'', NULL, ''flight'')';

         --ALTER TABLE flight.distances DROP COLUMN ORIGIN;
         --ALTER TABLE flight.distances DROP COLUMN DEST;
         --ALTER TABLE flight.schedule DROP COLUMN OP_CARRIER;
        ---drop table res_linregr_summary, res_linregr
        EXECUTE 'DROP TABLE IF EXISTS join_result';
        EXECUTE 'DROP TABLE IF EXISTS train_table';
        _start_ts  := clock_timestamp();

        FOREACH col IN ARRAY columns_null
        LOOP
            _query_2 := _query_2 || format('(CASE WHEN %s IS NOT NULL THEN false ELSE true END) AS %s_is_null, ', col, col);
        end loop;

        --_query_2 := RTRIM(_query_2, ', ');

        EXECUTE 'CREATE TEMPORARY TABLE tmp_table AS (SELECT * FROM (flight.schedule JOIN flight.distances USING(airports)) AS s JOIN flight.flights USING (flight))';
        _int_ts  := clock_timestamp();

        _imputed_data := standard_imputation(num_columns_null, cat_columns_null);

        columns_no_null := array_diff(columns,  num_columns_null || cat_columns_null);

        ---select values with averages
        FOR _counter in 1..(cardinality(columns_no_null))
        LOOP
            _query := _query || '%s, ';
            params := params || columns_no_null[_counter];
        end loop;

        FOR _counter in 1..(cardinality(columns_null))
        LOOP
            _query := _query || '  COALESCE (%s, %s) AS %s, ';
            IF _counter <= cardinality(num_columns_null) then
                params := params || num_columns_null[_counter] || _imputed_data[_counter]::text || num_columns_null[_counter];
            ELSE
                params := params || cat_columns_null[_counter - cardinality(num_columns_null)] || _imputed_data[_counter]::text || cat_columns_null[_counter - cardinality(num_columns_null)];
            end if;
        end loop;

        RAISE NOTICE '%', ('CREATE TEMPORARY TABLE join_result AS (' || format(RTRIM(_query || _query_2, ', '), VARIADIC params) || ' FROM tmp_table)');
        ---train
        EXECUTE ('CREATE TEMPORARY TABLE join_result AS (' || format(RTRIM(_query || _query_2, ', '), VARIADIC params) || ' FROM tmp_table)');
        EXECUTE ('DROP TABLE tmp_table');

        FOR _counter in 1..mice_iter
        LOOP
            RAISE NOTICE 'mice iteration... %', _counter;
                FOR _counter_2 in 1..cardinality(num_columns_null)
                LOOP
                    RAISE NOTICE 'imputing... %', num_columns_null[_counter_2];
                    _indep_vars := ARRAY['1'] || array_remove(columns,  num_columns_null[_counter_2]);

                    EXECUTE ('CREATE TEMPORARY TABLE train_table AS (SELECT * FROM join_result WHERE ' || num_columns_null[_counter_2] || '_is_null IS false)');

                    --RAISE NOTICE '%', ('SELECT madlib.linregr_train( ''train_table'', ''res_linregr'' ,''' || num_columns_null[_counter_2] || ''', ''ARRAY[' || array_to_string(_indep_vars, ', ') || ']'')');

                    EXECUTE ('SELECT madlib.linregr_train( ''train_table'', ''res_linregr'' ,''' ||
                             num_columns_null[_counter_2] || ''', ''ARRAY[' || array_to_string(_indep_vars, ', ') || ']'')');

                    RAISE NOTICE 'trained, predicting... %', num_columns_null[_counter_2];

                    --- predict
                    _query := ('SELECT '||array_to_string(_indep_vars,', ') ||', madlib.linregr_predict( m.coef, ' || ' ARRAY[' || array_to_string(_indep_vars, ', ') || ']) as predict FROM join_result, res_linregr m WHERE ' || num_columns_null[_counter_2] || '_is_null IS true');
                    --RAISE NOTICE '%', _query;
                    --RAISE NOTICE '--DONE';

                    --RAISE NOTICE '%', ('UPDATE train_table SET train_table.' || num_columns_null[_counter_2] || ' = p.predict FROM (' || _query || ') AS p WHERE p.id = train_table.id');
                    EXECUTE ('UPDATE join_result SET ' || num_columns_null[_counter_2] || ' = p.predict FROM (' || _query || ') AS p WHERE p.id = join_result.id');
                    --RAISE NOTICE 'DROP TABLE res_linregr';
                    EXECUTE ('DROP TABLE res_linregr');
                    EXECUTE 'DROP TABLE res_linregr_summary';
                    EXECUTE 'DROP TABLE train_table';
                    --RAISE NOTICE '------DONE-----';
                END LOOP;


            FOR _counter_2 in 1..cardinality(cat_columns_null)
                LOOP
                    RAISE NOTICE 'imputing... %', cat_columns_null[_counter_2];
                    _indep_vars := ARRAY['1'] || array_remove(columns,  cat_columns_null[_counter_2]);
                    --RAISE NOTICE '%', ('SELECT madlib.linregr_train( ''train_table'', ''res_linregr'' ,''' || num_columns_null[_counter_2] || ''', ''ARRAY[' || array_to_string(_indep_vars, ', ') || ']'')');

                    EXECUTE ('CREATE TEMPORARY TABLE train_table AS (SELECT * FROM join_result WHERE ' || num_columns_null[_counter_2] || '_is_null IS false)');

                    EXECUTE ('SELECT madlib.logregr_train( ''train_table'', ''res_logregr'' ,''' ||
                             cat_columns_null[_counter_2] || ''', ''ARRAY[' || array_to_string(_indep_vars, ', ') || ']'')');

                    --- predict
                    _query := ('SELECT '||array_to_string(_indep_vars,', ') ||', madlib.logregr_predict( m.coef, ' || ' ARRAY[' || array_to_string(_indep_vars, ', ') || ']) as predict FROM join_result, res_logregr m WHERE ' || cat_columns_null[_counter_2] || '_is_null IS true');
                    --RAISE NOTICE '%', _query;
                    --RAISE NOTICE '--DONE';

                    --RAISE NOTICE '%', ('UPDATE train_table SET train_table.' || num_columns_null[_counter_2] || ' = p.predict FROM (' || _query || ') AS p WHERE p.id = train_table.id');
                    EXECUTE ('UPDATE join_result SET ' || cat_columns_null[_counter_2] || ' = p.predict::int FROM (' || _query || ') AS p WHERE p.id = join_result.id');
                    --RAISE NOTICE 'DROP TABLE res_linregr';
                    EXECUTE ('DROP TABLE res_logregr');
                    EXECUTE 'DROP TABLE res_logregr_summary';
                    EXECUTE 'DROP TABLE train_table';

                    --RAISE NOTICE '------DONE-----';
                END LOOP;
        end loop;
        _end_ts  := clock_timestamp();
        RAISE NOTICE 'Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
        RAISE NOTICE 'Execution time (JOIN) in ms = %' , 1000 * (extract(epoch FROM _int_ts - _start_ts));

        ---create tmp table missing values and imputed values
        --FOR _counter in 1..cardinality(columns_null)
        --LOOP
        --    perform create_table_missing_values(columns,  columns_null[_counter]);
        --   perform create_table_imputed_values(columns_null[_counter], _imputed_data[_counter]);
        --end loop;
    return 0;
    END;
$$ LANGUAGE plpgsql;

--- CREATE TEMPORARY TABLE join_result AS (SELECT * FROM (flight.schedule JOIN flight.distances USING(airports)) AS s JOIN flight.flights USING (flight))


----for mindsdb
---todo update
CREATE OR REPLACE FUNCTION mean_imputation() RETURNS int AS $$
    DECLARE
        num_columns_null text[] := ARRAY['CRS_DEP_HOUR', 'DISTANCE', 'TAXI_OUT', 'TAXI_IN', 'ARR_DELAY', 'DEP_DELAY'];
        cat_columns_null text[] := ARRAY['DIVERTED'];
        columns_null text[] := num_columns_null || cat_columns_null;
        columns text[] := ARRAY['id', 'airports', 'DISTANCE', 'flight', 'CRS_DEP_HOUR', 'CRS_DEP_MIN', 'CRS_ARR_HOUR', 'CRS_ARR_MIN'];
        _imputed_data float8[];
        _counter int;
        _query text := 'SELECT ';
        col text;
        _query_2 text := '';
        columns_no_null text[];
        params text[];
    BEGIN
        EXECUTE 'DROP TABLE IF EXISTS join_result';
        EXECUTE 'DROP TABLE IF EXISTS train_table';

        FOREACH col IN ARRAY columns_null
        LOOP
            _query_2 := _query_2 || format('(CASE WHEN %s IS NOT NULL THEN false ELSE true END) AS %s_is_null, ', col, col);
        end loop;


        EXECUTE 'CREATE TABLE tmp_table AS (SELECT * FROM (flight.schedule JOIN flight.distances USING(airports)) AS s JOIN flight.flights USING (flight))';
        _imputed_data := standard_imputation(num_columns_null, cat_columns_null);
        columns_no_null := array_diff(columns,  num_columns_null || cat_columns_null);
        FOR _counter in 1..(cardinality(columns_no_null))
        LOOP
            _query := _query || '%s, ';
            params := params || columns_no_null[_counter];
        end loop;
        FOR _counter in 1..(cardinality(columns_null))
        LOOP
            _query := _query || '  COALESCE (%s, %s) AS %s, ';
            IF _counter <= cardinality(num_columns_null) then
                params := params || num_columns_null[_counter] || _imputed_data[_counter]::text || num_columns_null[_counter];
            ELSE
                params := params || cat_columns_null[_counter - cardinality(num_columns_null)] || _imputed_data[_counter]::text || cat_columns_null[_counter - cardinality(num_columns_null)];
            end if;
        end loop;
        EXECUTE ('CREATE TABLE join_result AS (' || format(RTRIM(_query || _query_2, ', '), VARIADIC params) || ' FROM tmp_table)');
        EXECUTE ('DROP TABLE tmp_table');
    return 0;
    END;
$$ LANGUAGE plpgsql;
