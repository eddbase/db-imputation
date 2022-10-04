--computes average for each col_nan

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
    query := RTRIM(query, ', ') || '] FROM %s%s;';

    RAISE NOTICE '%', format(query, VARIADIC columns || tbl.schema || '.' || tbl.name);

    EXECUTE format(query, VARIADIC columns || tbl.schema || '.' || tbl.name) INTO avg_imputations;
   --_end_ts := clock_timestamp();
    --RAISE NOTICE 'AVG Imputation %', 1000 * (extract(epoch FROM _end_ts - _start_ts));
   RETURN avg_imputations;
  END;
$$ LANGUAGE plpgsql;

--generates a cofactor matrix for a single table
'''
CREATE OR REPLACE FUNCTION generate_partial_cofactor_helper(_table t_info, col_imputing text) RETURNS text AS $$
DECLARE
    _triple Triple;
    col text;
    query text;
    _counter int;
    min_col_id text := format('tmp_min%s_%s.%s', _table.name, col_imputing, _table.identifier);
    params text[] := ARRAY[]::text[];
    join_params_query text := '';
    pos_col_imputing int;
BEGIN

    FOREACH col IN ARRAY _table.join_keys
    LOOP
        join_params_query := join_params_query || '%s , ';
    end loop;
    params := params || format(join_params_query, VARIADIC _table.join_keys);

    if array_position(_table.columns_null, col_imputing) IS NULL THEN
        pos_col_imputing := -1;
    else
        pos_col_imputing := array_position(_table.columns_null, col_imputing);
    end if;

    query := generate_cofactor_query(cardinality(_table.columns_null), cardinality(_table.columns_no_null), pos_col_imputing);

    --build params
    FOREACH col IN ARRAY _table.columns_null
    LOOP
        IF col = col_imputing THEN
            params := params || format('tmp_imputed_%s.%s', col_imputing, col_imputing);
        ELSE
            params := params || format('tmp_min%s_%s.%s', _table.name, col_imputing, col) || format('tmp_min%s_%s.%s', _table.name, col_imputing, col) || format('tmp_imputed_%s.%s', col, col);
        end if;
    end loop;
    FOREACH col IN ARRAY _table.columns_no_null
    LOOP
        params := params || format('tmp_min%s_%s.%s', _table.name, col_imputing, col);
    end loop;
    query := query || ' AS cof_%s FROM (tmp_min%s_%s JOIN tmp_imputed_%s ON %s = tmp_imputed_%s.%s )';
    params := params || _table.name || _table.name || col_imputing || col_imputing || min_col_id || col_imputing || ARRAY['id'];--todo change id with table-specific id

    FOREACH col IN ARRAY _table.columns_null
    LOOP
        IF col != col_imputing THEN
            query := query || ' LEFT OUTER JOIN tmp_imputed_%s ON %s = tmp_imputed_%s.%s ';
            params := params || col || min_col_id || col || ARRAY['id'];
        end if;
    end loop;

    IF cardinality(_table.join_keys) > 0 THEN
        query := query || 'GROUP BY %s';
        params := params || format(RTRIM(join_params_query, ', '), VARIADIC _table.join_keys);
    end if;

    RAISE NOTICE 'LIFT QUERY %', format(query, VARIADIC params);
    --EXECUTE format(query, VARIADIC params) INTO _triple;
    RETURN format(query, VARIADIC params);
END;
$$ LANGUAGE plpgsql;

--generates a cofactor matrix for a subset of rows

CREATE OR REPLACE FUNCTION generate_partial_cofactor(tables t_info[], col_imputing text, join_condition text[] DEFAULT ARRAY[]::text[]) RETURNS Triple AS $$
DECLARE
    _triple Triple;
    i int;
    query text := 'SELECT SUM(';
    tbl t_info;
BEGIN
    FOREACH tbl IN ARRAY tables
    LOOP
        query := query || format('q_%s.cof_%s * ', tbl.name, tbl.name);
    end loop;
    query := RTRIM(query, '* ') || format(') FROM (%s) AS q_%s ', generate_partial_cofactor_helper(tables[1], col_imputing), tables[1].name);
    for i in 2 .. cardinality(tables)
    LOOP
        query := query || format(' JOIN (%s) AS q_%s ON %s  ', generate_partial_cofactor_helper(tables[i], col_imputing), tables[i].name, join_condition[i-1]);
    end loop;
    RAISE NOTICE '%', query;
    --EXECUTE query INTO _triple;
    return _triple;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION generate_full_cofactor_helper(_table t_info, averages float8[]) RETURNS text AS $$
DECLARE
    --_triple Triple;
    col text;
    query text;
    _counter int;
    params text[] := ARRAY[]::text[];
    join_params_query text := '';
BEGIN
    FOREACH col IN ARRAY _table.join_keys
    LOOP
        join_params_query := join_params_query || '%s , ';
    end loop;
    params := params || format(join_params_query, VARIADIC _table.join_keys);

    query := generate_cofactor_query(cardinality(_table.columns_null), cardinality(_table.columns_no_null));
    query := query || ' AS cof_%s FROM %s ';
    --build params
    FOR _counter in 1..cardinality(_table.columns_null)
    LOOP
        params := params || _table.columns_null[_counter] || _table.columns_null[_counter] || averages[_counter]::text;
    end loop;
    FOREACH col IN ARRAY _table.columns_no_null
    LOOP
        params := params || col;
    end loop;

    params := params || _table.name || (_table.schema || _table.name);

    IF cardinality(_table.join_keys) > 0 THEN
        query := query || 'GROUP BY %s';
        params := params || format(RTRIM(join_params_query, ', '), VARIADIC _table.join_keys);
    end if;

    RAISE NOTICE 'LIFT QUERY %', format(query, VARIADIC params || (_table.schema || _table.name));
    --EXECUTE format(query, VARIADIC params || (_schema || table_n)) INTO _triple;
    RETURN format(query, VARIADIC params || (_table.schema || _table.name)); --_triple;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION generate_full_cofactor(tables t_info[], averages float8[], join_condition text[] DEFAULT ARRAY[]::text[]) RETURNS Triple AS $$
DECLARE
    _triple Triple;
    i int;
    query text := 'SELECT SUM(';
    tbl t_info;
    cnt_columns_prev int;
BEGIN
    FOREACH tbl IN ARRAY tables
    LOOP
        query := query || format('q_%s.cof_%s * ', tbl.name, tbl.name);
    end loop;
    cnt_columns_prev := cardinality(tables[1].columns_null);
    query := RTRIM(query, '* ') || format(') FROM (%s) AS q_%s ', generate_full_cofactor_helper(tables[1], averages[1:cnt_columns_prev]), tables[1].name);
    for i in 2 .. cardinality(tables)
    LOOP
        query := query || format(' JOIN (%s) AS q_%s ON %s  ', generate_full_cofactor_helper(tables[i], averages[cnt_columns_prev+1:cnt_columns_prev+cardinality(tables[i].columns_null)]), tables[i].name, join_condition[i-1]);
        cnt_columns_prev := cnt_columns_prev + cardinality(tables[i].columns_null);
    end loop;
    RAISE NOTICE '%', query;
    --EXECUTE query INTO _triple;
    return _triple;
END;
$$ LANGUAGE plpgsql;
'''
---generate cofactor for a single table with no push of cofactor matrix
CREATE OR REPLACE FUNCTION generate_cofactor_helper(_table t_info, averages float8[]) RETURNS text AS $$
DECLARE
    --_triple Triple;
    col text;
    query text := 'SELECT %s SUM (';
    _counter int;
    params text[] := ARRAY[]::text[];
    join_params_query text := '';
BEGIN
    FOREACH col IN ARRAY _table.join_keys
    LOOP
        join_params_query := join_params_query || '%s , ';
    end loop;
    params := params || format(join_params_query, VARIADIC _table.join_keys);

    query := generate_cofactor_query(cardinality(_table.columns_null), cardinality(_table.columns_no_null));
    query := query || ' AS cof_%s FROM %s ';
    --build params
    FOR _counter in 1..cardinality(_table.columns_null)
    LOOP
        params := params || _table.columns_null[_counter] || _table.columns_null[_counter] || averages[_counter]::text;
    end loop;
    FOREACH col IN ARRAY _table.columns_no_null
    LOOP
        params := params || col;
    end loop;

    params := params || _table.name || (_table.schema || _table.name);

    IF cardinality(_table.join_keys) > 0 THEN
        query := query || 'GROUP BY %s';
        params := params || format(RTRIM(join_params_query, ', '), VARIADIC _table.join_keys);
    end if;

    RAISE NOTICE 'LIFT QUERY %', format(query, VARIADIC params || (_table.schema || _table.name));
    --EXECUTE format(query, VARIADIC params || (_schema || table_n)) INTO _triple;
    RETURN format(query, VARIADIC params || (_table.schema || _table.name)); --_triple;
END;
$$ LANGUAGE plpgsql;

--generate cofactor entry function

CREATE OR REPLACE FUNCTION generate_cofactor(tables t_info[], averages float8[], join_condition text[] DEFAULT ARRAY[]::text[]) RETURNS Triple AS $$
DECLARE
    _triple Triple;
    i int;
    query text := 'SELECT SUM(';
    query_from text;
    tbl t_info;
    col text;
    col_nulls text[];
    col_not_null text[];
    cnt_columns_prev int;
BEGIN
    FOREACH tbl IN ARRAY tables
    LOOP
        IF tbl.push_cofactor THEN--multiply cofactor matrix
            query := query || format('q_%s.cof_%s * ', tbl.name, tbl.name);
            query_from := query_from || generate_cofactor_helper(tbl, averages[1:cnt_columns_prev]);
            cnt_columns_prev := cnt_columns_prev + cardinality(tables[i].columns_null);
        ELSE--lift columns
            col_nulls := col_nulls || tbl.columns_null;
            col_not_null := col_not_null || tbl.columns_no_null;
        END IF;
    end loop;

    if array_position(col_nulls, col_imputing) IS NULL THEN
        pos_col_imputing := -1;
    else
        pos_col_imputing := array_position(tbl.columns_null, col_imputing);
    end if;
    --query: lift cofactors * generate lift5(...)*...*liftx(...)
    query := query || generate_cofactor_query(cardinality(tbl.columns_null), cardinality(tbl.columns_no_null), pos_col_imputing);

    --join section
    --join the subquery with groupby
    cnt_columns_prev := cardinality(tables[1].columns_null);
    query := RTRIM(query, '* ') || format(') FROM (%s) AS q_%s ', generate_cofactor_helper(tables[1], averages[1:cnt_columns_prev]), tables[1].name);
    for i in 2 .. cardinality(tables)
    LOOP
        query := query || format(' JOIN (%s) AS q_%s ON %s  ', generate_cofactor_helper(tables[i], averages[cnt_columns_prev+1:cnt_columns_prev+cardinality(tables[i].columns_null)]), tables[i].name, join_condition[i-1]);
        cnt_columns_prev := cnt_columns_prev + cardinality(tables[i].columns_null);
    end loop;

    --join tables lifted here
    --todo

    RAISE NOTICE '%', query;
    --EXECUTE query INTO _triple;
    return _triple;
END;
$$ LANGUAGE plpgsql;

--skip_null: treats a null as %s
CREATE OR REPLACE FUNCTION generate_cofactor_query(_columns_null int, _columns_no_null int, skip_null int DEFAULT -1) RETURNS text AS $$
DECLARE
    _query text := '';
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
                _query := _query || '(CASE WHEN %s IS NOT NULL THEN %s ELSE %s END), ';--todo update
            else
                _query := _query || '%s , ';
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
            _query := _query || '(CASE WHEN %s IS NOT NULL THEN %s ELSE %s END ), ';
        else
            _query := _query || '%s , ';
        end if;
        _query := RTRIM(_query, ', ') || ')';
    else
        _query := RTRIM(_query, '*');
    end if;
    _query := _query || ')';
    RETURN _query;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION create_table_missing_values(tbl t_info, col_nan text) RETURNS void AS $$
  DECLARE
    col text;
    columns_no_null text[];
    insert_query text := 'CREATE TEMPORARY TABLE tmp_min%s_%s AS SELECT ';
  BEGIN
    columns_no_null := array_remove(tbl.columns_null || tbl.columns_no_null,  col_nan);
    FOREACH col IN ARRAY columns_no_null
    LOOP
        insert_query := insert_query || '%s, ';
    end loop;
    insert_query := RTRIM(insert_query, ', ') || ' FROM %s WHERE %s IS NULL';
    EXECUTE format('DROP TABLE IF EXISTS tmp_min%s_%s', tbl.name, col_nan);
    RAISE NOTICE 'create_table_missing_values:  %', format(insert_query, VARIADIC ARRAY[tbl.name, col_nan] || columns_no_null || ARRAY[(tbl.schema || '.' || tbl.name), col_nan]);
    EXECUTE format(insert_query, VARIADIC ARRAY[tbl.name, col_nan] || columns_no_null || ARRAY[(tbl.schema || '.' || tbl.name), col_nan]);
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION create_table_imputed_values(tbl t_info, col_nan text, average float8) RETURNS void AS $$
  DECLARE
      col text;
    insert_query text := 'CREATE TEMPORARY TABLE tmp_imputed_%s AS SELECT ';
  BEGIN

    FOREACH col IN ARRAY tbl.identifier
    LOOP
        insert_query := insert_query || '%s, ';
    end loop;
    insert_query := insert_query || '%s AS %s FROM tmp_min%s_%s';
    EXECUTE format('DROP TABLE IF EXISTS tmp_imputed_%s', col_nan);
    RAISE NOTICE 'create_table_imputed_values:  %', format(insert_query, VARIADIC ARRAY[col_nan] || tbl.identifier || average::text || col_nan || tbl.name || col_nan);
    EXECUTE format(insert_query, VARIADIC ARRAY[col_nan] || tbl.identifier || average::text || col_nan || tbl.name || col_nan);
  END;
$$ LANGUAGE plpgsql;

--todo

CREATE OR REPLACE FUNCTION update_table_cofactor(table_imputing t_info, tables t_info[], col_missing text, weights float8[], join_cols text[]) RETURNS void AS $$
  DECLARE
    _start_ts timestamptz;
    _end_ts timestamptz;
      query text := 'INSERT INTO tmp_imputed_%s SELECT %s, SUM(%s)/SUM(%s) FROM %s GROUP BY %s';---select key_table, sum(Q1.count*Q2.count*Q3.sum + Q1.count*Q2.sum*Q3.count + Q1.sum*Q2.count*Q3.count)/sum(Q1.count*Q2.count*Q3.sum + Q1.count*Q2.count*Q3.count) FROM (SUBQUERYS GROUP BY JOIN KEY (+ID FOR IMPUTING TABLE)) GROUP BY key_imputed_table
      query_compute_avg_num text;
    query_compute_avg_den text;
      col text;
      tbl t_info;
      keys_query text;
        tbl_1 t_info;
      count_cols int := 1;
      count_tbl int := 1;
      from_query text;
  BEGIN
    EXECUTE format('TRUNCATE TABLE tmp_imputed_%s', col_missing);
    FOREACH tbl IN ARRAY tables
    LOOP
        ---multiply col with weights
        --imputation denumerator
        query_compute_avg_den := query_compute_avg_den || format('%s.c * ', tbl.name);
        --add other_tables.count*other_tables.count*current_table_query.sum + ... (imputation numerator)
        query_compute_avg_num := query_compute_avg_num || format('(%s.s * ', tbl.name);
        FOREACH tbl_1 IN ARRAY tables--sum of this table * count of other tables
        LOOP
            query_compute_avg_num := query_compute_avg_num || format('%s.c * ', tbl_1.name);
        end loop;
        query_compute_avg_num := RTRIM(query_compute_avg_num, '* ') || ') + ';
        from_query := from_query || update_table_cofactor_helper(tbl, col_missing, weights[count_cols:count_cols+cardinality(tbl.columns_null)+cardinality(tbl.columns_no_null)]);
        if count_cols == 1 THEN
            from_query := from_query || format(' AS q_%s JOIN ', tbl.name);
        else
            from_query := from_query || format(' AS q_%s ON %s JOIN ', tbl.name, join_cols[count_tbl]);
        end if;
        count_tbl := count_tbl + 1;
        count_cols := count_cols+cardinality(tbl.columns_null)+cardinality(tbl.columns_no_null);
    end loop;

    from_query := RTRIM(from_query, 'JOIN ');

    FOREACH col IN ARRAY table_imputing.identifier
    LOOP
        keys_query := keys_query || format('%s, ', col);
    end loop;
    keys_query := RTRIM(keys_query, ', ');

    ---select computed
    query := format(query, col_missing || keys_query || RTRIM(query_compute_avg_num, '+ ') || RTRIM(query_compute_avg_den, '* ') || from_query || keys_query);

  END;
$$ LANGUAGE plpgsql;

---multiply weights and cols
CREATE OR REPLACE FUNCTION update_table_cofactor_helper(_table t_info, col_missing text, weights float8[]) RETURNS text AS $$
  DECLARE
    _query text := 'SELECT ';
    _join_keys text;
    params text[];
    _counter int;
    col text;
    min_col_id text;
    identifier text;
    _start_ts timestamptz;
    _end_ts timestamptz;
  BEGIN

    FOREACH col IN ARRAY _table.join_keys
    LOOP
        _join_keys := _join_keys || format('%s, ', col);
    end loop;
    _join_keys := RTRIM(_join_keys, ', ');
    _query := _query || _join_keys || format(', COUNT(*) AS c, SUM(');

      --if value is null, multiply imputed value in imputed table
    FOR _counter in 1..cardinality(_table.columns_null)
    LOOP
        if _table.columns_null[_counter] != col_missing then
            _query := _query || ' ((CASE WHEN tmp_min%s_%s.%s IS NOT NULL THEN tmp_min%s_%s.%s ELSE tmp_imputed_%s.%s END) * %s ) +';
            params := params || _table.name || col_missing || _table.columns_null[_counter] || _table.name || col_missing || _table.columns_null[_counter] || _table.columns_null[_counter] || _table.columns_null[_counter] || weights[_counter+1]::text;
        end if;
    end LOOP;

    ---else multiply non null values
    FOR _counter in 1..cardinality(_table.columns_no_null)
    LOOP
        _query := _query || '(tmp_min%s_%s.%s * %s) + ';
        params := params || _table.name || col_missing || _table.columns_no_null[_counter] || weights[cardinality(_table.columns_null) + _counter + 1]::text;
    end LOOP;

    ---as col_missing
    _query := _query || ' %s )) AS s';
    params := params || weights[1]::text;

    --from

    _query := _query || ' FROM tmp_min%s_%s';
    params := params || _table.name || col_missing;

    ---write join between imputed data tables
    FOREACH col IN ARRAY _table.columns_null
    LOOP
        IF col != col_missing THEN
            _query := _query || format(' LEFT OUTER JOIN tmp_imputed_%s ON ', col);
            FOREACH identifier IN ARRAY _table.identifier
            LOOP
                _query := _query || format('tmp_min%s_%s.%s = tmp_imputed_%s.%s AND ', _table.name, col_missing, identifier, col, identifier);
            end loop;
            _query := RTRIM(_query, 'AND ');
        end if;
    end loop;
    _start_ts := clock_timestamp();
    RAISE NOTICE '%', format(_query, VARIADIC params);
    --EXECUTE format(_query, VARIADIC params);
    _end_ts := clock_timestamp();
    _query := _query || ' GROUP BY ' || _join_keys;
    return format(_query, VARIADIC params);
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION impute(tables t_info[], imputing_table t_info, join_cols text[], iterations int) RETURNS int AS $$
  DECLARE
    _triple Triple;
    _triple_removal Triple;
    _triple_train Triple;
    _label_idx int;
    col text;
    --no_id_columns_no_null text[] := array_remove(columns_no_null, identifier);
    _columns text[] := _nan_cols || columns_no_null;--always null || no null
    _params float8[];
    _imputed_data float8[];
   _start_ts timestamptz;
   _end_ts   timestamptz;
    tbl t_info;
    cofactor_partial_query text[];
  BEGIN

    FOREACH tbl IN ARRAY tables
    LOOP
        _imputed_data := standard_imputation(tbl);
        ---create tmp table missing values and imputed values
        FOR _counter in 1..cardinality(tbl.columns_null)
        LOOP
            perform create_table_missing_values(tbl,  tbl.columns_null[_counter]);
            perform create_table_imputed_values(tbl, tbl.columns_null[_counter], _imputed_data[_counter]);
        end loop;

        cofactor_partial_query := cofactor_partial_query || generate_cofactor_helper(tbl, _imputed_data);

    END LOOP;

    --_triple := generate_full_cofactor(table_n, _schema || '.', no_id_columns_no_null, _nan_cols, _imputed_data, '');

    ---lift tables

    _triple := format('SELECT SUM(cof_Inventory * cof_Item * cof_Location * cof_Weather * cof_Census) FROM (%s) AS Q1 JOIN (%s) AS Q2 ON Q1.ksn = Q2.ksn', );

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

            perform update_table_cofactor(imputing_table, tables, col, _params, join_cols);

            _triple := _triple_train + generate_partial_cofactor(table_n, no_id_columns_no_null, _nan_cols, col, 'id');
            _end_ts   := clock_timestamp();
            RAISE NOTICE 'Impute new values and update cofactor matrix: Execution time in ms = %' , 1000 * (extract(epoch FROM _end_ts - _start_ts));
            end loop;
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
      --define tables
    tbls := tbls || row('Inventory', 'retailer', '{"inventoryunits"}', '{}', '{"locn", "dateid", "ksn"}', '{"ksn"}')::t_info;
    tbls := tbls || row('Location', 'retailer', '{"zip","rgn_cd","clim_zn_nbr","tot_area_sq_ft","sell_area_sq_ft","avghhi","supertargetdistance","supertargetdrivetime","targetdistance","targetdrivetime","walmartdistance","walmartdrivetime","walmartsupercenterdistance","walmartsupercenterdrivetime"}', '{}', 'locn', '{"zip"}')::t_info;
    tbls := tbls || row('Weather', 'retailer', '{"rain","snow","maxtemp","mintemp","meanwind","thunder"}', '{}', '{"locn", "dateid"}', '{"locn"}')::t_info;
    tbls := tbls || row('Item', 'retailer', '{"subcategory","category","categoryCluster","prize"}', '{}', 'ksn', '{"ksn"}')::t_info;
    tbls := tbls || row('Census', 'retailer', '{"zip","population","white","asian","pacific","black","medianage","occupiedhouseunits","houseunits","families","households","husbwife","males","females","householdschildren","hispanic"}', '{}', 'zip', '{"zip"}')::t_info;

    t_impute := row('Inventory', 'retailer', '{"inventoryunits"}', '{}', '{"locn", "dateid", "ksn"}', '{"ksn"}')::t_info;

    join_tables := ARRAY['Inventory.locn = Location.locn', 'Inventory.locn = Weather.locn AND Inventory.dateid = Weather.dateid', 'Inventory.ksn = Retailer.ksn', 'Census.zip = Location.zip'];

    return impute(tbls, t_impute, join_tables,1);
  END;
$$ LANGUAGE plpgsql;

--TEST QUERY

SELECT standard_imputation('inventory', ARRAY['locn', 'dateid']);
SELECT create_table_missing_values('inventory', ARRAY['locn', 'dateid', 'inventoryunits', 'ksn', 'id'], 'locn');
SELECT create_table_imputed_values('inventory', ARRAY['locn', 'dateid', 'inventoryunits', 'ksn', 'id'], 'locn');

--SELECT generate_full_cofactor('inventory', ARRAY['inventoryunits', 'ksn'], ARRAY['locn', 'dateid'], ARRAY[1,2], '');
create type t_info as (name text, schema text, columns_no_null text[], columns_null text[], identifier text[], join_keys text[], push_cofactor bool);

SELECT generate_full_cofactor(ARRAY[row('inventory', 'tmp', '{"ksn", "inventoryunits"}', '{"locn","dateid"}', 'id', '{"ksn"}')::t_info, row('Item', 'tmp', '{"subcategory", "category", "categorycluster", "prize"}', '{}', 'id', '{"ksn"}')::t_info], ARRAY[1,2,3], ARRAY[' q_inventory.ksn = q_Item.ksn']);
SELECT generate_partial_cofactor(ARRAY[row('inventory', 'tmp', '{"ksn", "inventoryunits"}', '{"locn","dateid"}', 'id', '{"ksn"}')::t_info, row('Item', 'tmp', '{"subcategory", "category", "categorycluster", "prize"}', '{}', 'id', '{"ksn"}')::t_info], 'locn', ARRAY[' q_inventory.ksn = q_Item.ksn']);


--SELECT generate_partial_cofactor('inventory', ARRAY['no_null_a', 'no_null_b', 'no_null_c', 'no_null_d'], ARRAY['null_a', 'null_b','null_c'], 'null_a', 'id');
SELECT update_table_cofactor('inventory', 'null_a', ARRAY['null_a', 'null_b','null_c'], ARRAY['no_null_a', 'no_null_b', 'no_null_c', 'no_null_d'], ARRAY[1,2,3,4,5,6,7], 'id');

SELECT impute('minventory', 'test',ARRAY['locn', 'dateid'], ARRAY['ksn', 'inventoryunits', 'id'], 1, 'id');
