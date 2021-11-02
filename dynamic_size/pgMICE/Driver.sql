CREATE OR REPLACE FUNCTION standard_imputation(table_n text, _duplicate_table text, _col_nan text[], _columns text[]) RETURNS Triple AS $$
  DECLARE
    _triple Triple;
    col text;
    _query text := 'SELECT SUM(LIFT(';
  BEGIN
    EXECUTE format('DROP TABLE IF EXISTS %s;', _duplicate_table);
    EXECUTE format('CREATE  TABLE %s AS (SELECT * FROM %s)', _duplicate_table, table_n);
    FOREACH col IN ARRAY _col_nan
    LOOP
        EXECUTE format('UPDATE %s SET %s = (SELECT AVG(%s) FROM %s) WHERE %s IS NULL;', _duplicate_table, col, col, _duplicate_table, col);--mean imputation
    end LOOP;

    --generate full cofactor matrix

    FOREACH col IN ARRAY _columns[:cardinality(columns)-1]
        LOOP
            _query := CONCAT(_query, col, ')*LIFT(');
    end LOOP;
    _query := _query || columns[cardinality(columns)] || ')) FROM %s;';

    EXECUTE format(_query, _duplicate_table) INTO _triple;
    RETURN _triple;
  END;
$$ LANGUAGE plpgsql;

select standard_imputation('Test3', 'Test3_dup', array['a', 'b', 'c', 'd']);

--create cofactor matrix of missing values

CREATE OR REPLACE FUNCTION cofactor_nan(table_n text, _duplicate_table text, col text, p_key text, columns text[]) RETURNS Triple AS $$
  DECLARE
    _triple Triple;
    _query text := 'SELECT SUM(LIFT(';
  BEGIN
    --col: col null, columns: columns lifted
    FOREACH col IN ARRAY columns[:cardinality(columns)-1]
        LOOP
            _query := CONCAT(_query, col, ')*LIFT(');
    end LOOP;
    _query := _query || columns[cardinality(columns)] || ')) FROM %s WHERE %s IN (SELECT %s FROM %s WHERE %s IS NULL);';
    raise notice '%', _query;
    --echo _query;
    EXECUTE format(_query, _duplicate_table, p_key, p_key, table_n, col) INTO _triple;
    return _triple;
  END;
$$ LANGUAGE plpgsql;

select cofactor_nan('Test3', 'Test3_dup', 'b', 'pkey', array['a','c','d']);

CREATE OR REPLACE FUNCTION update_table_cofactor(table_n text, _duplicate_table text, col_missing text, p_key text, columns text[], weights float8[]) RETURNS void AS $$
  DECLARE
    _triple Triple;
    _query text := 'UPDATE %s SET %s = subquery.result FROM (SELECT %s, ';
    _query_update_cofactor text := 'SELECT ';
    _update_cofactor1 float8[];
    _update_cofactor2 float8[];
    _counter int;
  BEGIN
        FOR _counter in 1..cardinality(columns)
        LOOP
            _query := CONCAT(_query, columns[_counter], '*', weights[_counter+1]::text, ' + ');
            _query_update_cofactor := CONCAT(_query_update_cofactor, 'SUM (', columns[_counter], '*', col_missing, '), ');
    end LOOP;
    _query := _query || weights[1] || ' AS result FROM %s WHERE %s IN (SELECT %s FROM %s WHERE %s IS NULL)) AS subquery WHERE %s.%s = subquery.%s;';
    raise notice '%', format(_query, _duplicate_table, col_missing, p_key, _duplicate_table, p_key, p_key, table_n, col_missing, _duplicate_table, p_key, p_key);
    _query_update_cofactor := CONCAT(_query_update_cofactor, ' FROM ', _duplicate_table, ' WHERE ', col_missing, ' IN (SELECT %s FROM %s WHERE %s IS NULL);');

    EXECUTE format(_query_update_cofactor, p_key, table_n, col_missing) INTO _update_cofactor1;
    -- UPDATE table_imputed SET imputed_col = (SELECT pkey, A*w[1]+B*w[2]+... AS result FROM table_imputed where primary_k IS IN (SELECT p_key FROM table_n WHERE col IS NULL)) WHERE p_key IN (SELECT p_key FROM table_n WHERE col IS NULL)
    EXECUTE format(_query, _duplicate_table, col_missing, p_key, _duplicate_table, p_key, p_key, table_n, col_missing, _duplicate_table, p_key, p_key);--update imputed values
    EXECUTE format(_query_update_cofactor, p_key, table_n, col_missing) INTO _update_cofactor2;
    --update cofactor matrix
    FOR _counter in 1..cardinality(columns)
        LOOP
            _update_cofactor2 := _update_cofactor2 - _update_cofactor1;
    end LOOP;

  END;
$$ LANGUAGE plpgsql;

select update_table_cofactor('Test3', 'Test3_dup', 'b', 'pkey', array['pkey','a'], array[1,1,1]);

CREATE OR REPLACE FUNCTION impute(table_n text, _duplicate_table text, _nan_cols text[], p_key text, columns text[]) RETURNS int AS $$
  DECLARE
    _triple Triple;
    _triple_removal Triple;
    _triple_train Triple;
    _label_idx int;
    _params double[];
    _imputed_data double[];
  BEGIN
    --   SELECT is_nullable, COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS GROUP BY is_nullable;
    _triple := standard_imputation(table_n, _duplicate_table, _nan_cols, columns);

    for counter in 1..2 loop
        FOREACH col IN ARRAY _nan_cols
        LOOP
            -- remove set of rows nan in A
            _triple_removal := cofactor_nan(table_n, _duplicate_table, col, p_key, columns);
            _triple_train := _triple - _triple_removal;
            -- fit model over cofactor matrix
            _label_idx := array_position(columns, col);
            _params := converge(_triple_train, _label_idx, 0.01, 0);
            --update cofactor matrix and table


        end LOOP;
    end loop;
    RETURN 0;
  END;
$$ LANGUAGE plpgsql;