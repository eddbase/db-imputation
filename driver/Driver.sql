CREATE OR REPLACE FUNCTION test() RETURNS void AS $$
DECLARE
   _start_ts timestamptz;
   _end_ts   timestamptz;
BEGIN
   _start_ts := clock_timestamp();
    EXECUTE format('SELECT SUM(lift5(A,B,C,D,E)*lift5(F,G,H,I,J)*lift5(K,L,M,N,O)*lift5(P,Q,R,S,T)*lift5(U,V,W,X,Y)*lift5(Z,AA,BB,CC,DD)) FROM t_random;');
    _end_ts := clock_timestamp();
    RAISE NOTICE 'Test: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION prepare_imputation(table_n text, _col_nan text[]) RETURNS void AS $$
DECLARE
    col text;
    _nan_cols_names text[];
   _start_ts timestamptz;
   _end_ts   timestamptz;
    _query_update text := 'UPDATE %s SET ';
BEGIN
   _start_ts := clock_timestamp();
    FOREACH col IN ARRAY _col_nan
    LOOP
        EXECUTE format('ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s BOOLEAN DEFAULT FALSE;', table_n, col || '_is_nan');
        RAISE NOTICE 'ALTER TABLE ADD COLUMN IF NOT EXISTS';
        _query_update := CONCAT(_query_update, '%s = (%s IS NULL), ');
        _nan_cols_names := _nan_cols_names || ARRAY[col || '_is_nan', col];
    end LOOP;
    _query_update := RTRIM(_query_update, ', ') || ';';
    RAISE NOTICE '%', _query_update;
    EXECUTE format(_query_update, VARIADIC ARRAY[table_n] || _nan_cols_names);--, col || '_is_nan'
    FOREACH col IN ARRAY _col_nan
    LOOP
        EXECUTE format('CREATE INDEX %s_index ON %s (%s) WHERE %s is TRUE', col || '_is_nan', table_n, col || '_is_nan', col || '_is_nan');
    end LOOP;
    --EXECUTE format('UPDATE %s SET %s = true WHERE (%s is NULL);', table_n, col || '_is_nan', col);--, col || '_is_nan'
    --RAISE NOTICE 'UPDATE SET true WHERE ( is NULL AND is NULL);';
    --EXECUTE format('CREATE INDEX %s_index ON %s (%s) WHERE %s is TRUE', col || '_is_nan', table_n, col || '_is_nan', col || '_is_nan');
    --RAISE NOTICE 'CREATE INDEX s_index ON WHERE is TRUE';
    --end LOOP;
   _end_ts := clock_timestamp();
    RAISE NOTICE 'Prepare imputation. Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION create_triple_query(_columns int) RETURNS text AS $$
DECLARE
    _query text := 'SELECT SUM(LIFT(';
    col text;
    counter int;
BEGIN
    FOR counter in 1..(_columns-1)
    LOOP
        _query := CONCAT(_query, '%s)*LIFT(');
    end LOOP;
    _query := _query || '%s)) FROM %s';
    RETURN _query;
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION standard_imputation(table_n text, _col_nan text[]) RETURNS void AS $$
  DECLARE
    _start_ts timestamptz;
    _end_ts timestamptz;
    col text;
  BEGIN
   _start_ts := clock_timestamp();
    FOREACH col IN ARRAY _col_nan
    LOOP
        EXECUTE format('UPDATE %s SET %s = (SELECT AVG(%s) FROM %s WHERE %s IS FALSE) WHERE %s IS TRUE;', table_n, col, col, table_n, col || '_is_nan', col || '_is_nan');--mean imputation
    end LOOP;
   _end_ts := clock_timestamp();
    RAISE NOTICE 'AVG Imputation %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION generate_full_cofactor(table_n text, _columns text[]) RETURNS Triple AS $$
DECLARE
    _start_ts timestamptz;
    _end_ts timestamptz;
    _triple Triple;
    _query text;
BEGIN
    _query := create_triple_query(cardinality(_columns)) || ';';
    RAISE NOTICE '%',_query;
    _start_ts := clock_timestamp();
    EXECUTE format(_query, VARIADIC _columns || table_n) INTO _triple;
   _end_ts := clock_timestamp();
    RAISE NOTICE 'Full cofactor matrix %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
    RETURN _triple;
END;
$$ LANGUAGE plpgsql;

--create cofactor matrix of missing values

CREATE OR REPLACE FUNCTION cofactor_nan(table_n text, col text, columns text[]) RETURNS Triple AS $$
  DECLARE
    _triple Triple;
    _query text;
    _col_iter text;
  BEGIN
    _query := create_triple_query(cardinality(columns)) || ' WHERE %s IS true;';
    EXECUTE format(_query, VARIADIC columns || ARRAY[table_n, col || '_is_nan']) INTO _triple;
    return _triple;
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_table_cofactor(table_n text, col_missing text, columns text[], weights float8[]) RETURNS void AS $$
  DECLARE
    _query text := 'UPDATE %s SET %s = ';
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
    _start_ts := clock_timestamp();
    EXECUTE format(_query, table_n, col_missing, col_missing || '_is_nan');--update imputed values
    _end_ts := clock_timestamp();
    RAISE NOTICE 'Impute new values. Time: = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
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
    perform standard_imputation(table_n, _nan_cols);
    _triple := generate_full_cofactor(table_n, columns);
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
            -- fit model over cofactor matrix
            _label_idx := array_position(columns, col)-1;--c code

            _start_ts := clock_timestamp();
            _params := converge(_triple_train, _label_idx, 0.001, 0, 10000);
            _end_ts   := clock_timestamp();
            RAISE NOTICE 'Train the model: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));

            --update cofactor matrix and table
            _start_ts := clock_timestamp();
            perform update_table_cofactor(table_n, col, columns, _params);
            _triple := _triple_train + cofactor_nan(table_n, col, columns);
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

select prepare_imputation('inventory', ARRAY['date']);
select impute('inventory', array['store', 'date'], array['store','date','item','units'], 1);
