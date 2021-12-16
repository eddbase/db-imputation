CREATE OR REPLACE FUNCTION standard_imputation(table_n text, _col_nan text[], _schema text) RETURNS float8[] AS $$
  DECLARE
    _start_ts timestamptz;
    _end_ts timestamptz;
    col text;
    columns text[];
    avg_imputations float8[];
    query text := 'SELECT ARRAY[';
  BEGIN
   _start_ts := clock_timestamp();
    FOREACH col IN ARRAY _col_nan
    LOOP
        query := query || 'AVG(CASE WHEN %s IS NOT NULL THEN %s END), ';
        columns := columns || col || col;
    end LOOP;
    query := RTRIM(query, ', ') || '] FROM %s%s;';

    RAISE NOTICE '%', format(query, VARIADIC columns || _schema || table_n);

    EXECUTE format(query, VARIADIC columns || _schema || table_n) INTO avg_imputations;
   --_end_ts := clock_timestamp();
    --RAISE NOTICE 'AVG Imputation %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
   RETURN avg_imputations;
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION generate_partial_cofactor(table_n text, _columns_no_null text[], _columns_null text[], col_imputing text, identifier text, join_key text DEFAULT '') RETURNS Triple AS $$
DECLARE
    _triple Triple;
    col text;
    query text;
    _counter int;
    min_col_id text := format('tmp_min%s_%s.%s', table_n, col_imputing, identifier);
    params text[] := ARRAY[join_key];
BEGIN
    query := generate_cofactor_query(cardinality(_columns_null), cardinality(_columns_no_null), array_position(_columns_null, col_imputing));
    --build params
    FOREACH col IN ARRAY _columns_null
    LOOP
        IF col = col_imputing THEN
            params := params || format('tmp_imputed_%s.%s', col_imputing, col_imputing);
        ELSE
            params := params || format('tmp_min%s_%s.%s', table_n, col_imputing, col) || format('tmp_min%s_%s.%s', table_n, col_imputing, col) || format('tmp_imputed_%s.%s', col, col);
        end if;
    end loop;
    FOREACH col IN ARRAY _columns_no_null
    LOOP
        params := params || format('tmp_min%s_%s.%s', table_n, col_imputing, col);
    end loop;

    query := query || ' FROM (tmp_min%s_%s JOIN tmp_imputed_%s ON %s = tmp_imputed_%s.%s )';
    params := params || table_n || col_imputing || col_imputing || min_col_id || col_imputing || ARRAY['id'];--todo change id with table-specific id

    FOREACH col IN ARRAY _columns_null
    LOOP
        IF col != col_imputing THEN
            query := query || ' LEFT OUTER JOIN tmp_imputed_%s ON %s = tmp_imputed_%s.%s ';
            params := params || col || min_col_id || col || ARRAY['id'];
        end if;
    end loop;

    query := query || ';';

    RAISE NOTICE 'LIFT QUERY %', format(query, VARIADIC params);
    EXECUTE format(query, VARIADIC params) INTO _triple;
    RETURN _triple;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION generate_full_cofactor(table_n text, _schema text, _columns_no_null text[], _columns_null text[], averages float8[], join_key text DEFAULT '') RETURNS Triple AS $$
DECLARE
    _triple Triple;
    col text;
    query text;
    _counter int;
    params text[] := ARRAY[join_key];
BEGIN
    query := generate_cofactor_query(cardinality(_columns_null), cardinality(_columns_no_null));
    query := query || ' FROM %s';
    --build params
    FOR _counter in 1..cardinality(_columns_null)
    LOOP
        params := params || _columns_null[_counter] || _columns_null[_counter] || averages[_counter]::text;
    end loop;
    FOREACH col IN ARRAY _columns_no_null
    LOOP
        params := params || col;
    end loop;
    RAISE NOTICE 'LIFT QUERY %', format(query, VARIADIC params || (_schema || table_n));
    EXECUTE format(query, VARIADIC params || (_schema || table_n)) INTO _triple;
    RETURN _triple;
END;
$$ LANGUAGE plpgsql;

--skip_null: treats a null as %s
CREATE OR REPLACE FUNCTION generate_cofactor_query(_columns_null int, _columns_no_null int, skip_null int DEFAULT -1) RETURNS text AS $$
DECLARE
    _query text := 'SELECT %s SUM (';
    n_lift5 int := div(_columns_null + _columns_no_null, 5);
    _counter_mod int := MOD(_columns_null + _columns_no_null, 5);
    _counter_lift_5 int;
    _counter int;
BEGIN
    FOR _counter_lift_5 in 0..n_lift5-1
    LOOP
        _query := _query || 'lift5(';
        FOR _counter in (_counter_lift_5*5)+1..((_counter_lift_5+1)*5)
        LOOP
            IF _counter <= _columns_null AND _counter != skip_null THEN
                _query := _query || '(CASE WHEN %s IS NOT NULL THEN %s ELSE %s END), ';
            else
                _query := _query || '%s, ';
            end if;
        end loop;
        _query := RTRIM(_query, ', ') || ')*';
    end loop;

    IF _counter_mod > 1 THEN
        _query := _query || 'lift' || _counter_mod || '(';
        FOR _counter in 1.._counter_mod
        LOOP
            IF (_counter + (n_lift5*5)) <= _columns_null AND (_counter+(n_lift5*5)) != skip_null THEN
                _query := _query || '(CASE WHEN %s IS NOT NULL THEN %s ELSE %s END), ';
            else
                _query := _query || '%s, ';
            end if;
        end loop;
        _query := RTRIM(_query, ', ') || ')';
    elsif _counter_mod = 1 THEN
        _query := _query || 'lift(';
        IF n_lift5*5 <= _columns_null AND ((n_lift5*5)+1) != skip_null THEN --if I have more nulls than nlift5*5
            _query := _query || '(CASE WHEN %s IS NOT NULL THEN %s ELSE %s END), ';
        else
            _query := _query || '%s, ';
        end if;
        _query := RTRIM(_query, ', ') || ')';
    else
        _query := RTRIM(_query, '*');
    end if;
    _query := _query || ')';
    RETURN _query;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION create_table_missing_values(table_n text, _columns text[], col_nan text, _schema text) RETURNS void AS $$
  DECLARE
    col text;
    columns_no_null text[];
    insert_query text := 'CREATE TEMPORARY TABLE tmp_min%s_%s AS SELECT ';
  BEGIN
    columns_no_null := array_remove(_columns, col_nan);
    FOREACH col IN ARRAY columns_no_null
    LOOP
        insert_query := insert_query || '%s, ';
    end loop;
    insert_query := RTRIM(insert_query, ', ') || ' FROM %s WHERE %s IS NULL';
    EXECUTE format('DROP TABLE IF EXISTS tmp_min%s_%s', table_n, col_nan);
    RAISE NOTICE '%', format(insert_query, VARIADIC ARRAY[table_n, col_nan] || columns_no_null || ARRAY[(_schema || table_n), col_nan]);
    EXECUTE format(insert_query, VARIADIC ARRAY[table_n, col_nan] || columns_no_null || ARRAY[(_schema || table_n), col_nan]);
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION create_table_imputed_values(table_n text, col_nan text, average float8, identifier text) RETURNS void AS $$
  DECLARE
    insert_query text := 'CREATE TEMPORARY TABLE tmp_imputed_%s AS SELECT %s, %s AS %s FROM tmp_min%s_%s';
  BEGIN
    EXECUTE format('DROP TABLE IF EXISTS tmp_imputed_%s', col_nan);
    RAISE NOTICE '%', format(insert_query, VARIADIC ARRAY[col_nan] || identifier || average::text || col_nan || table_n || col_nan);
    EXECUTE format(insert_query, VARIADIC ARRAY[col_nan] || identifier || average::text || col_nan || table_n || col_nan);
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_table_cofactor(table_n text, col_missing text, columns_null text[], columns_no_null text[], weights float8[], identifier text) RETURNS void AS $$
  DECLARE
    _query text := 'INSERT INTO tmp_imputed_%s SELECT tmp_min%s_%s.%s , (';--tmp_minventory_dateid.id, (...) AS
    params text[] := ARRAY [col_missing, table_n, col_missing, identifier];
    _counter int;
    col text;
    min_col_id text := format('tmp_min%s_%s.%s', table_n, col_missing, identifier);
    _start_ts timestamptz;
    _end_ts timestamptz;
  BEGIN
    EXECUTE format('TRUNCATE TABLE tmp_imputed_%s', col_missing);

    FOR _counter in 1..cardinality(columns_null)
    LOOP
        if columns_null[_counter] != col_missing then
            --(CASE WHEN tmp_minventory_dateid.locn IS NOT NULL THEN tmp_minventory_dateid.locn  ELSE tmp_imputed_locn.locn  END)
            _query := _query || ' ((CASE WHEN tmp_min%s_%s.%s IS NOT NULL THEN tmp_min%s_%s.%s ELSE tmp_imputed_%s.%s END) * %s ) +';
            params := params || table_n || col_missing || columns_null[_counter] || table_n || col_missing || columns_null[_counter] || columns_null[_counter] || columns_null[_counter] || weights[_counter+1]::text;
        end if;
    end LOOP;

    FOR _counter in 1..cardinality(columns_no_null)
    LOOP
        _query := _query || '(tmp_min%s_%s.%s * %s) + ';
        params := params || table_n || col_missing || columns_no_null[_counter] || weights[cardinality(columns_null) + _counter + 1]::text;
    end LOOP;

    _query := _query || ' %s ) AS %s';
    params := params || weights[1]::text || col_missing;

    --from

    _query := _query || ' FROM tmp_min%s_%s';
    params := params || table_n || col_missing;

    FOREACH col IN ARRAY columns_null
    LOOP
        IF col != col_missing THEN
            _query := _query || ' LEFT OUTER JOIN tmp_imputed_%s ON %s = tmp_imputed_%s.%s ';
            params := params || col || min_col_id || col || ARRAY['id'];
        end if;
    end loop;

    _query := _query || ';';


    _start_ts := clock_timestamp();
    RAISE NOTICE '%', format(_query, VARIADIC params);
    --EXECUTE format(_query, VARIADC params
    _end_ts := clock_timestamp();
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION impute(table_n text, _schema text, _nan_cols text[], columns_no_null text[], iterations int, identifier text) RETURNS int AS $$
  DECLARE
    _triple Triple;
    _triple_removal Triple;
    _triple_train Triple;
    _label_idx int;
    col text;
    no_id_columns_no_null text[] := array_remove(columns_no_null, identifier);
    _columns text[] := _nan_cols || columns_no_null;--always null || no null
    _params float8[];
    _imputed_data float8[];
   _start_ts timestamptz;
   _end_ts   timestamptz;
  BEGIN

    _imputed_data := standard_imputation(table_n, _nan_cols, _schema || '.');

    FOR _counter in 1..cardinality(_nan_cols)
    LOOP
        perform create_table_missing_values(table_n, _columns, _nan_cols[_counter], _schema || '.');
        perform create_table_imputed_values(table_n, _nan_cols[_counter], _imputed_data[_counter], identifier);
    end loop;

    _triple := generate_full_cofactor(table_n, _schema || '.', no_id_columns_no_null, _nan_cols, _imputed_data, '');

    for counter in 1..iterations
        loop
        FOREACH col IN ARRAY _nan_cols
            LOOP
            _start_ts := clock_timestamp();
            _triple_removal := generate_partial_cofactor(table_n, no_id_columns_no_null, _nan_cols, col, 'id');
            _end_ts   := clock_timestamp();
            RAISE NOTICE 'Generate cofactor of nan values: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
            _triple_train := _triple - _triple_removal;
            -- fit model over cofactor matrix
            _label_idx := array_position(_columns, col)-1;--c code

            _start_ts := clock_timestamp();
            _params := converge(_triple_train, _label_idx, 0.001, 0, 10000);

            _end_ts   := clock_timestamp();
            RAISE NOTICE 'Train the model: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));

            --update cofactor matrix and table
            _start_ts := clock_timestamp();
            perform update_table_cofactor(table_n, col, _nan_cols, no_id_columns_no_null, _params, 'id');
            _triple := _triple_train + generate_partial_cofactor(table_n, no_id_columns_no_null, _nan_cols, col, 'id');
            _end_ts   := clock_timestamp();
            RAISE NOTICE 'Impute new values and update cofactor matrix: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
            end loop;
        end loop;
    RETURN 0;
  END;
$$ LANGUAGE plpgsql;

--TEST QUERY

SELECT standard_imputation('inventory', ARRAY['locn', 'dateid']);
SELECT create_table_missing_values('inventory', ARRAY['locn', 'dateid', 'inventoryunits', 'ksn', 'id'], 'locn');
SELECT create_table_imputed_values('inventory', ARRAY['locn', 'dateid', 'inventoryunits', 'ksn', 'id'], 'locn');

SELECT generate_full_cofactor('inventory', ARRAY['inventoryunits', 'ksn'], ARRAY['locn', 'dateid'], ARRAY[1,2], '');
SELECT generate_full_cofactor('inventory', ARRAY['no_null_a', 'no_null_b', 'no_null_c', 'no_null_d'], ARRAY['null_a', 'null_b','null_c'], ARRAY[1,2,3], '');
SELECT generate_partial_cofactor('inventory', ARRAY['no_null_a', 'no_null_b', 'no_null_c', 'no_null_d'], ARRAY['null_a', 'null_b','null_c'], 'null_a', 'id');
SELECT update_table_cofactor('inventory', 'null_a', ARRAY['null_a', 'null_b','null_c'], ARRAY['no_null_a', 'no_null_b', 'no_null_c', 'no_null_d'], ARRAY[1,2,3,4,5,6,7], 'id');

SELECT impute('minventory', 'test',ARRAY['locn', 'dateid'], ARRAY['ksn', 'inventoryunits', 'id'], 1, 'id');
