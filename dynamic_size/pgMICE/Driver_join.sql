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
   _end_ts := clock_timestamp();
    RAISE NOTICE 'Prepare imputation. Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION standard_imputation(table_n text, _col_nan text[], pkey text) RETURNS void AS $$
  DECLARE
    _start_ts timestamptz;
    _end_ts timestamptz;
    col text;
  BEGIN
   _start_ts := clock_timestamp();
    FOREACH col IN ARRAY _col_nan
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS imputed_%s', col);
        EXECUTE format('CREATE TABLE imputed_%s(id integer, value float);', col);
        EXECUTE format('INSERT INTO imputed_%s (SELECT %s, (SELECT AVG(%s) from %s WHERE %s_is_nan IS FALSE) from %s WHERE %s_is_nan IS TRUE);', col, pkey, col, table_n, col, table_n, col);
        RAISE NOTICE 'Insert into';
        EXECUTE format('CREATE INDEX imputed_%s_index ON imputed_%s (id)', col, col);
        RAISE NOTICE 'Create index';
    end LOOP;
   _end_ts := clock_timestamp();
    RAISE NOTICE 'AVG Imputation %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION reconstruct_matrix(table_n text, _col_nan text[], _columns_non_nan text[], pkey text, keep_pkey boolean, lift boolean) RETURNS text AS $$
  DECLARE
    _query text := 'SELECT ';
    left_join text[];
      col text;
      counter int;
      col_nans text[];
  BEGIN

      IF keep_pkey THEN
          _query := _query || table_n || '.' || pkey || ' , ';
      end if;

      IF lift THEN
            _query := _query || 'SUM(';
            FOR counter in 1..(cardinality(_columns_non_nan))
            LOOP
                _query := CONCAT(_query, 'LIFT(%s)*');
            end LOOP;
        ELSE
            FOR counter in 1..(cardinality(_columns_non_nan))
            LOOP
                _query := CONCAT(_query, ' %s, ');
            end LOOP;
      end if;

    IF lift THEN
        FOREACH col IN ARRAY _col_nan
        LOOP
            _query := CONCAT(_query, 'LIFT (CASE WHEN %s_is_nan is True THEN imputed_%s.value ELSE %s END)*');
            col_nans := col_nans || col || col || col;
        end LOOP;
        _query := trim(trailing  '*' from _query) || ')';
    ELSE
        FOREACH col IN ARRAY _col_nan
        LOOP
            _query := CONCAT(_query, ' CASE	WHEN %s_is_nan is True THEN imputed_%s.value ELSE %s END %s, ');
            col_nans := col_nans || col || col || col || col;
        end LOOP;
        _query := trim(trailing  ', ' from _query);
    END if;

   _query := _query || ' FROM %s ';

    FOREACH col IN ARRAY _col_nan
    LOOP
        _query := CONCAT(_query, ' LEFT OUTER JOIN imputed_%s ON %s.%s = imputed_%s.id ');
        left_join := left_join || col || table_n || pkey || col;
    end LOOP;

   --RAISE NOTICE '%', format(_query, VARIADIC _columns_non_nan || col_nans || table_n || left_join );
    return format(_query, VARIADIC _columns_non_nan || col_nans || table_n || left_join );
    --RETURN _query;
  END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION generate_full_cofactor(table_n text, _col_nan text[], _columns_non_nan text[], pkey text) RETURNS Triple AS $$
DECLARE
    _start_ts timestamptz;
    _end_ts timestamptz;
    _triple Triple;
    _query text;
BEGIN
    _query := reconstruct_matrix(table_n, _col_nan, _columns_non_nan, pkey, keep_pkey := false, lift := true) || ';';
    RAISE NOTICE 'GENERATE FULL COFACTOR %',_query;
    _start_ts := clock_timestamp();
    EXECUTE _query INTO _triple;
   _end_ts := clock_timestamp();
    RAISE NOTICE 'Full cofactor matrix %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
    RETURN _triple;
END;
$$ LANGUAGE plpgsql;

--create cofactor matrix of missing values

CREATE OR REPLACE FUNCTION cofactor_nan(table_n text, _col_nan text[], _columns_non_nan text[], current_col_missing text, pkey text) RETURNS Triple AS $$
  DECLARE
    _triple Triple;
    _query text;
    _col_iter text;
  BEGIN
    _query := reconstruct_matrix(table_n, _col_nan, _columns_non_nan, pkey, keep_pkey := false, lift := true) || ' WHERE %s IS true;';
    RAISE NOTICE '%', format(_query, current_col_missing || '_is_nan');
    EXECUTE format(_query, current_col_missing || '_is_nan') INTO _triple;
    return _triple;
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_table_cofactor(table_n text, col_missing text, columns_not_nan text[], columns_nan text[], weights float8[], pkey text) RETURNS void AS $$
  DECLARE
    _query text := 'INSERT INTO imputed_%s (SELECT %s, ';
    _counter int;
    _start_ts timestamptz;
    _end_ts timestamptz;
      columns text[] := columns_not_nan || columns_nan;
      from_query text;
  BEGIN

    FOR _counter in 1..cardinality(columns)
    LOOP
        --RAISE NOTICE 'WEIGHT % ', weights[_counter+1];
        if columns[_counter] != col_missing then
            _query := CONCAT(_query, columns[_counter], '*', weights[_counter+1]::text, ' + ');
        end if;
    end LOOP;

    from_query := reconstruct_matrix(table_n, columns_nan, columns_not_nan, pkey, keep_pkey := true, lift := false) || ' WHERE %s is True';
    --RAISE NOTICE 'QUERY %',_query;
    --RAISE NOTICE 'QUERY %',from_query;
    _query := CONCAT(_query, weights[1]::text, ' FROM (', from_query, ') full_table)');

    --RAISE NOTICE 'QUERY %',_query;

    _start_ts := clock_timestamp();
    EXECUTE format('DROP TABLE imputed_%s', col_missing);
    EXECUTE format('CREATE TEMPORARY TABLE imputed_%s(id integer, value float);', col_missing);
    RAISE NOTICE '%', format(_query, col_missing, pkey, col_missing || '_is_nan');
    EXECUTE format(_query, col_missing, pkey, col_missing || '_is_nan');
    EXECUTE format('CREATE INDEX imputed_%s_index ON imputed_%s (id)', col_missing, col_missing);
    _end_ts := clock_timestamp();
    RAISE NOTICE 'Impute new values. Time: = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION impute(table_n text, _nan_cols text[], _non_nan_columns text[], iterations int, pkey text) RETURNS int AS $$
  DECLARE
    _triple Triple;
    _triple_removal Triple;
    _triple_train Triple;
    _label_idx int;
    col text;
      columns text[] := _non_nan_columns || _nan_cols;
    _params float8[];
    _imputed_data float8[];

   _start_ts timestamptz;
   _end_ts   timestamptz;
  BEGIN
    --   SELECT is_nullable, COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS GROUP BY is_nullable;
   _start_ts := clock_timestamp();
    perform standard_imputation(table_n, _nan_cols, pkey);
    _triple := generate_full_cofactor(table_n, _nan_cols, _non_nan_columns, pkey);--lift + reconstruct
   _end_ts   := clock_timestamp();
   RAISE NOTICE 'Impute data and generate full triple: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
    for counter in 1..iterations loop
        FOREACH col IN ARRAY _nan_cols
        LOOP
            -- remove set of rows nan in A
            _start_ts := clock_timestamp();
            _triple_removal := cofactor_nan(table_n, _nan_cols, _non_nan_columns, col, pkey);
            _end_ts   := clock_timestamp();
            RAISE NOTICE 'Generate cofactor of nan values: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
            _triple_train := _triple - _triple_removal;
            --RAISE NOTICE ' % % % ', _triple, _triple_removal, _triple_train;
            -- fit model over cofactor matrix
            _label_idx := array_position(columns, col)-1;--c code

            _start_ts := clock_timestamp();
            _params := converge(_triple_train, _label_idx, 0.001, 0, 10000);
            --RAISE NOTICE ' % % ', _triple_train, _label_idx;
            _end_ts   := clock_timestamp();
            RAISE NOTICE 'Train the model: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));

            --update cofactor matrix and table
            _start_ts := clock_timestamp();
            perform update_table_cofactor(table_n, col, _non_nan_columns, _nan_cols, _params, pkey);
            _triple := _triple_train + cofactor_nan(table_n, _nan_cols, _non_nan_columns, col, pkey);
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
select standard_imputation('Test3', ARRAY['b','c','d'],'pkey');
select update_table_cofactor('Test3', 'b', ARRAY['a'], ARRAY['b','c','d'], ARRAY[1,2,3,4], 'pkey');
select impute('Test3', array['b','c','d'], array['a'], 1, 'pkey');

select prepare_imputation('inventory', ARRAY['store','date']);
select impute('inventory', array['store', 'date'], array['item','units'], 1, 'id');

--