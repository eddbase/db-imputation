CREATE OR REPLACE FUNCTION prepare_imputation(table_n text, _col_nan text[]) RETURNS void AS $$
DECLARE
    col text;
   _start_ts timestamptz;
   _end_ts   timestamptz;
BEGIN
   _start_ts := clock_timestamp();
    FOREACH col IN ARRAY _col_nan
    LOOP
        EXECUTE format('ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s BOOLEAN DEFAULT FALSE;', table_n, col || '_is_nan');
        RAISE NOTICE 'ALTER TABLE ADD COLUMN IF NOT EXISTS';
        EXECUTE format('UPDATE %s SET %s = true WHERE (%s is NULL);', table_n, col || '_is_nan', col);--, col || '_is_nan'
        RAISE NOTICE 'UPDATE SET true WHERE ( is NULL AND is NULL);';
        EXECUTE format('CREATE INDEX %s_index ON %s (%s) WHERE %s is TRUE', col || '_is_nan', table_n, col || '_is_nan', col || '_is_nan');
        RAISE NOTICE 'CREATE INDEX s_index ON WHERE is TRUE';
        --EXECUTE format('UPDATE %s SET %s = false WHERE (%s is NOT NULL AND %s is NULL);', table_n, col || '_is_nan', col, col || '_is_nan');
        --RAISE NOTICE 'UPDATE SET true WHERE ( is NULL AND is NULL);';
    end LOOP;
   _end_ts := clock_timestamp();
    RAISE NOTICE 'Prepare imputation. Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION standard_imputation(table_n text, _col_nan text[], _columns text[]) RETURNS Triple AS $$
  DECLARE
    _start_ts timestamptz;
    _end_ts timestamptz;
    _triple Triple;
    col text;
    _query text := 'SELECT SUM(LIFT(';
  BEGIN
   _start_ts := clock_timestamp();
    FOREACH col IN ARRAY _col_nan
    LOOP
        EXECUTE format('UPDATE %s SET %s = (SELECT AVG(%s) FROM %s WHERE %s IS FALSE) WHERE %s IS TRUE;', table_n, col, col, table_n, col || '_is_nan', col || '_is_nan');--mean imputation
    end LOOP;
   _end_ts := clock_timestamp();
    RAISE NOTICE 'AVG Imputation %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
    --generate full cofactor matrix
    FOREACH col IN ARRAY _columns[:cardinality(_columns)-1]
        LOOP
            _query := CONCAT(_query, col, ')*LIFT(');
    end LOOP;
    _query := _query || _columns[cardinality(_columns)] || ')) FROM %s;';
    _start_ts := clock_timestamp();
    EXECUTE format(_query, table_n) INTO _triple;
   _end_ts := clock_timestamp();
    RAISE NOTICE 'Full cofactor matrix %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
    RETURN _triple;
  END;
$$ LANGUAGE plpgsql;

--create cofactor matrix of missing values

CREATE OR REPLACE FUNCTION cofactor_nan(table_n text, col text, columns text[]) RETURNS Triple AS $$
  DECLARE
    _triple Triple;
    _query text := 'SELECT SUM(LIFT(';
    _col_iter text;
  BEGIN
    --col: col null, columns: columns lifted
    FOREACH _col_iter IN ARRAY columns[:cardinality(columns)-1]
        LOOP
            _query := CONCAT(_query, _col_iter, ')*LIFT(');
    end LOOP;
    _query := _query || columns[cardinality(columns)] || ')) FROM %s WHERE %s IS true;';
    --raise notice '%', format(_query, table_n, col || '_is_nan');
    --echo _query;
    EXECUTE format(_query, table_n, col || '_is_nan') INTO _triple;
    return _triple;
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_table_cofactor(table_n text, col_missing text, columns text[], weights float8[]) RETURNS Triple AS $$
  DECLARE
    _triple Triple;
    _query text := 'UPDATE %s SET %s = ';
    _query_update_triple text := 'SELECT SUM(LIFT(';
    _counter int;
    _start_ts timestamptz;
    _end_ts timestamptz;
  BEGIN
    FOR _counter in 1..cardinality(columns)
    LOOP
        if columns[_counter] != col_missing then
            _query := CONCAT(_query, columns[_counter], '*', weights[_counter+1]::text, ' + ');
        end if;
    end LOOP;
    _query := _query || weights[1] || ' WHERE %s IS TRUE;';
    --raise notice '%', format(_query, table_n, col_missing, p_key, table_n, col_missing || '_is_nan', table_n, p_key, p_key);
    _start_ts := clock_timestamp();
    EXECUTE format(_query, table_n, col_missing, col_missing || '_is_nan');--update imputed values
    _end_ts := clock_timestamp();
    RAISE NOTICE 'Impute new values. Time: = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
    _start_ts := clock_timestamp();
    _triple := cofactor_nan(table_n, col_missing, columns);
    _end_ts := clock_timestamp();
    RAISE NOTICE 'cofactor nan. Time: = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
    return _triple;
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION impute(table_n text, _nan_cols text[], columns text[], iterations int) RETURNS int AS $$
  DECLARE
    _triple Triple;
    _triple_removal Triple;
    _triple_train Triple;
    _label_idx int;
    col text;
    _params float8[];
    _imputed_data float8[];

   _start_ts timestamptz;
   _end_ts   timestamptz;
  BEGIN
    --   SELECT is_nullable, COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS GROUP BY is_nullable;
   _start_ts := clock_timestamp();
    _triple := standard_imputation(table_n, _nan_cols, columns);
    --RAISE NOTICE '%' , _triple;
   _end_ts   := clock_timestamp();
   RAISE NOTICE 'Impute data and generate full triple: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
    for counter in 1..iterations loop
        FOREACH col IN ARRAY _nan_cols
        LOOP
            -- remove set of rows nan in A
            _start_ts := clock_timestamp();
            _triple_removal := cofactor_nan(table_n, col, columns);
            _end_ts   := clock_timestamp();
            RAISE NOTICE 'Generate cofactor of nan values: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
            _triple_train := _triple - _triple_removal;
            --RAISE NOTICE '%' , _triple;
            --RAISE NOTICE '%' , _triple_train;
            --RAISE NOTICE '%' , _triple_removal;
            -- fit model over cofactor matrix
            _label_idx := array_position(columns, col)-1;--c code
            --RAISE NOTICE '% LABEL: %' , col, _label_idx;

            _start_ts := clock_timestamp();
            _params := converge(_triple_train, _label_idx, 0.001, 0, 10000);
            _end_ts   := clock_timestamp();
            RAISE NOTICE 'Train the model: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));

            --update cofactor matrix and table
            _start_ts := clock_timestamp();
            _triple_removal := update_table_cofactor(table_n, col, columns, _params);
            _triple := _triple_train + _triple_removal;
            _end_ts   := clock_timestamp();
            RAISE NOTICE 'Impute new values and update cofactor matrix: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));

        end LOOP;
    end loop;
    RETURN 0;
  END;
$$ LANGUAGE plpgsql;

select standard_imputation('Test3', array['b', 'c', 'd'], array['a', 'b', 'c', 'd']);
select cofactor_nan('Test3', 'b', array['a','c','d']);
select update_table_cofactor('Test3', 'b', array['a', 'b', 'c'], array[1,1,1,1]);

select prepare_imputation('Test4', ARRAY['b']);
select impute('Test4', array['b'], array['a','b'], 10);

select prepare_imputation('Test3', ARRAY['b','c','d']);
select impute('Test3', array['b','c','d'], array['a','b','c','d'], 1);

select prepare_imputation('inventory', ARRAY['store','date']);
select impute('inventory', array['store', 'date'], array['store','date','item','units'], 1);
