create type t_info as (name text, schema text, columns_no_null text[], columns_null text[], identifier text[], join_keys text[]);

--create type comp_tree as (leaf1 comp_tree, leaf2 comp_tree);

--table names: tmp_tablename_colname: table keeping null rows of colname
--tmp_imputed_colname: table keeping imputed values: keys, imputed value

CREATE OR REPLACE FUNCTION standard_imputation(tbl t_info) RETURNS float8[] AS $$
  DECLARE
    col text;
    columns text[];
    avg_imputations float8[];
    query text := 'SELECT ARRAY[';
  BEGIN
    FOREACH col IN ARRAY tbl.columns_null
    LOOP
        query := query || 'AVG(CASE WHEN %s IS NOT NULL THEN %s END), ';
        columns := columns || col || col;
    end LOOP;
    query := RTRIM(query, ', ') || '] FROM %s%s;';
    RAISE NOTICE '%', format(query, VARIADIC columns || tbl.schema || '.' || tbl.name);
    EXECUTE format(query, VARIADIC columns || tbl.schema || '.' || tbl.name) INTO avg_imputations;
   RETURN avg_imputations;
  END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION create_table_missing_values(tbl t_info, col_nan text) RETURNS void AS $$
  DECLARE
    col text;
    columns_no_null text[];
    insert_query text := 'CREATE TEMPORARY TABLE tmp_%s_%s AS SELECT ';
  BEGIN
    columns_no_null := array_remove(tbl.columns_null || tbl.columns_no_null,  col_nan);
    FOREACH col IN ARRAY columns_no_null
    LOOP
        insert_query := insert_query || '%s, ';
    end loop;
    insert_query := RTRIM(insert_query, ', ') || ' FROM %s WHERE %s IS NULL';
    EXECUTE format('DROP TABLE IF EXISTS tmp_%s_%s', tbl.name, col_nan);
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
    insert_query := insert_query || '%s AS %s FROM tmp_%s_%s';
    EXECUTE format('DROP TABLE IF EXISTS tmp_imputed_%s', col_nan);
    RAISE NOTICE 'create_table_imputed_values:  %', format(insert_query, VARIADIC ARRAY[col_nan] || tbl.identifier || average::text || col_nan || tbl.name || col_nan);
    EXECUTE format(insert_query, VARIADIC ARRAY[col_nan] || tbl.identifier || average::text || col_nan || tbl.name || col_nan);
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
            _query := _query || ' ((CASE WHEN tmp_%s_%s.%s IS NOT NULL THEN tmp_%s_%s.%s ELSE tmp_imputed_%s.%s END) * %s ) +';
            params := params || _table.name || col_missing || _table.columns_null[_counter] || _table.name || col_missing || _table.columns_null[_counter] || _table.columns_null[_counter] || _table.columns_null[_counter] || weights[_counter+1]::text;
        end if;
    end LOOP;

    ---else multiply non null values
    FOR _counter in 1..cardinality(_table.columns_no_null)
    LOOP
        _query := _query || '(tmp_%s_%s.%s * %s) + ';
        params := params || _table.name || col_missing || _table.columns_no_null[_counter] || weights[cardinality(_table.columns_null) + _counter + 1]::text;
    end LOOP;

    ---as col_missing
    _query := _query || ' %s )) AS s';
    params := params || weights[1]::text;

    --from

    _query := _query || ' FROM tmp_%s_%s';
    params := params || _table.name || col_missing;

    ---write join between imputed data tables
    FOREACH col IN ARRAY _table.columns_null
    LOOP
        IF col != col_missing THEN
            _query := _query || format(' LEFT OUTER JOIN tmp_imputed_%s ON ', col);
            FOREACH identifier IN ARRAY _table.identifier
            LOOP
                _query := _query || format('tmp_%s_%s.%s = tmp_imputed_%s.%s AND ', _table.name, col_missing, identifier, col, identifier);
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

--impute new values from weights
CREATE OR REPLACE FUNCTION update_table_cofactor(table_imputing t_info, tables t_info[], col_missing text, weights float8[], join_cols text[]) RETURNS void AS $$
  DECLARE
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
                _query := _query || ' coalesce (%s , %s ), ';--todo update
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
                _query := _query || ' coalesce (%s , %s ), ';
            else
                _query := _query || '%s, ';
            end if;
        end loop;
        _query := RTRIM(_query, ', ') || ')';
    elsif _counter_mod = 1 THEN
        _query := _query || 'lift(';
        IF n_lift5*5 <= _columns_null AND ((n_lift5*5)+1) != skip_null AND _columns_null > 0 THEN --if I have more nulls than nlift5*5
            _query := _query || ' coalesce (%s , %s ), ';
        else
            _query := _query || '%s , ';
        end if;
        _query := RTRIM(_query, ', ') || ')';
    else
        _query := RTRIM(_query, '*');
    end if;
return _query;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION generate_cofactor(root ltree, lift boolean, key text[], new_key text[], fill_nulls boolean, t_info hstore) RETURNS RECORD AS $$
DECLARE
    children ltree[];
    tname text := subpath(root, -1);
    tbl t_info[];
    child_lift boolean[];
    key_children text[];--2d array
    new_key_name text[];--2d array
    j int;
    ret RECORD;
    col text;
    col1 text;
    fill_null_children boolean[];
    child_columns_null_1 text[];
    child_columns_null_2 text[];
    child_columns_no_null text[];
    child_cofactor_names text[];
    columns_null text[];
    columns_null_query text[];
    columns_no_null text[];
    child_lifted text;
    child_from text;
    require_lift boolean := false;
    cofactor_keys text := '';
    group_by text := '';
    cofactor_names text[];
    cofactor_query text := '';
    from_section text;
BEGIN
    RAISE NOTICE 'KEY: % NEW KEY %', key, new_key;
    execute(format('SELECT array_agg(tname), array_agg(tbl), array_agg(lift), array_agg(key), array_agg(new_key_name), array_agg(fill_nulls) FROM join_tree WHERE tname ~ ''%s.*{1}''', root)) INTO children, tbl, child_lift, key_children, new_key_name, fill_null_children;
    RAISE NOTICE '% - % %', tname, key_children, new_key_name;

    IF children IS NOT NULL THEN--join node
        RAISE NOTICE 'Invoking % ', children[1]::ltree;
        SELECT A, B, C, D, E into child_columns_null_1, child_columns_no_null, child_cofactor_names, child_lifted, child_from FROM generate_cofactor(children[1]::ltree, child_lift[1],ARRAY(SELECT unnest(key_children[1:1])), ARRAY(SELECT unnest(new_key_name[1:1])), fill_null_children[1], t_info) AS (A text[], B text[], C text[], D text, E text);
        columns_null := columns_null || child_columns_null_1;
        columns_no_null := columns_no_null || child_columns_no_null;
        cofactor_names := cofactor_names || child_cofactor_names;
        IF child_lifted != '' THEN--need select, the query is a from
            from_section := format(' (SELECT %s FROM %s) AS %s JOIN ', child_lifted, child_from, subpath(children[1], -1));--select the columns from the lifted subtable
            require_lift := true;
        ELSE
            from_section := format(' %s JOIN ', child_from);--join the table and add the columns to the select part
        end if;

        RAISE NOTICE 'Invoking % ', children[2]::ltree;
        --same for child 2
        SELECT A, B, C, D, E into child_columns_null_2, child_columns_no_null, child_cofactor_names, child_lifted, child_from FROM generate_cofactor(children[2]::ltree, child_lift[2],  ARRAY(SELECT unnest(key_children[2:2])), ARRAY(SELECT unnest(new_key_name[2:2])), fill_null_children[2], t_info) AS (A text[], B text[], C text[], D text, E text);
        columns_null := columns_null || child_columns_null_2;
        columns_no_null := columns_no_null || child_columns_no_null;
        cofactor_names := cofactor_names || child_cofactor_names;
        IF child_lifted != '' THEN
            from_section := from_section || format(' (SELECT %s FROM %s) AS %s ON ', child_lifted, child_from, subpath(children[2], -1));
            require_lift := true;
        ELSE
            from_section := from_section || format(' %s ON ', child_from);
        end if;

        ---join columns
        for j in 1..cardinality(new_key_name[1:1]) loop
            IF new_key_name[1][j] IS NOT NULL AND new_key_name[2][j] IS NOT NULL THEN
                from_section := from_section || format('%s.%s = %s.%s AND ', subpath(children[1], -1), new_key_name[1][j], subpath(children[2], -1), new_key_name[2][j]);
            END IF;
        end loop;

        from_section := RTRIM(from_section, 'AND ');
        IF lift OR require_lift then--lift here
            --join imputed nulls
            FOREACH col IN ARRAY child_columns_null_1 LOOP
                columns_null_query := columns_null_query || col || format('tmp_imputed_%s.%s', col, col);
                from_section := from_section || format(' JOIN tmp_imputed_%s.%s ON ', col, col);
                FOREACH col1 IN ARRAY t_info->format('tmp_imputed_%s', col) LOOP--key of imputed table
                    from_section := from_section || format(' tmp_imputed_%s.%s = %s.%s AND ', col, col1, subpath(children[1], -1), col1);
                end loop;
                from_section := RTRIM(from_section, 'AND ');
            end loop;
            FOREACH col IN ARRAY child_columns_null_2 LOOP
                columns_null_query := columns_null_query || col || format('tmp_imputed_%s.%s', col, col);
                from_section := from_section || format(' JOIN tmp_imputed_%s.%s ON ', col, col);
                FOREACH col1 IN ARRAY t_info->format('tmp_imputed_%s', col) LOOP--key of imputed table
                    from_section := from_section || format(' tmp_imputed_%s.%s = %s.%s AND ', col, col1, subpath(children[2], -1), col1);
                end loop;
                from_section := RTRIM(from_section, 'AND ');
            end loop;
            RAISE NOTICE 'col nulls: %', cardinality(columns_null);
            RAISE NOTICE '% -- % -- %', generate_cofactor_query(cardinality(columns_null), cardinality(columns_no_null), -1), columns_null_query, columns_no_null;
            --
            --cofactor_query := 'SUM(';
            IF cardinality(columns_no_null) > 0 OR cardinality(columns_null) > 0 THEN--lift columns propagated from subqueries
                cofactor_query := cofactor_query || format(generate_cofactor_query(cardinality(columns_null), cardinality(columns_no_null), -1), VARIADIC columns_null_query || columns_no_null);
            END IF;
            --multiply by existing cofactor matrices
            FOREACH col IN ARRAY cofactor_names LOOP
                cofactor_query := cofactor_query || ' * ' || col;
            end loop;
            IF cardinality(columns_no_null) = 0 AND cardinality(columns_null) = 0 THEN-- avoid start with a * if no cols to lift
                cofactor_query := LTRIM(cofactor_query, ' * ');
                RAISE NOTICE 'NO NEW COLUMNS TO LIFT! %', cofactor_query;
            end if;
            RAISE NOTICE 'AAA';
            cofactor_query := 'SUM(' || cofactor_query || ')';

            --FOREACH col IN ARRAY key LOOP
            for j in 1..cardinality(key) loop--add columns if they are keys of this table
                IF new_key[j] IS NOT NULL THEN
                    cofactor_keys := cofactor_keys || format('%s AS %s, ', key[j], new_key[j]);
                    group_by := group_by || format('%s, ', key[j]);
                END IF;
            END LOOP;
            group_by := RTRIM(group_by, ', ');
            RAISE NOTICE 'AAA';
            IF group_by != '' THEN
                group_by := ' GROUP BY ' || group_by;
            end if;

            RAISE NOTICE 'GROUP BY OF JOIN %', group_by;

            ret := row(ARRAY[]::text[], ARRAY[]::text[], ARRAY['cof_'||tname], cofactor_keys || cofactor_query  || ' AS cof_' || tname, from_section || group_by);
        ELSE
            --could fill nulls
            IF fill_nulls THEN
                FOREACH col IN ARRAY child_columns_null_1 LOOP
                    columns_no_null := columns_no_null || format(' coalesce(%s.%s , tmp_imputed_%s.%s) ', subpath(children[1], -1), col, col, col);
                    from_section := from_section || format(' JOIN tmp_imputed_%s.%s ON ', col, col);
                    FOREACH col1 IN ARRAY t_info->format('tmp_imputed_%s', col) LOOP--key of imputed table
                        from_section := from_section || format(' tmp_imputed_%s.%s = %s.%s AND ', col, col1, subpath(children[1], -1), col1);
                    end loop;
                    from_section := RTRIM(from_section, 'AND ');
                end loop;

                FOREACH col IN ARRAY child_columns_null_2 LOOP
                    columns_no_null := columns_no_null || format(' coalesce(%s.%s , tmp_imputed_%s.%s) ', subpath(children[2], -1), col, col, col);
                    from_section := from_section || format(' JOIN tmp_imputed_%s.%s ON ', col, col);
                    FOREACH col1 IN ARRAY t_info->format('tmp_imputed_%s', col) LOOP--key of imputed table
                        from_section := from_section || format(' tmp_imputed_%s.%s = %s.%s AND ', col, col1, subpath(children[2], -1), col1);
                    end loop;
                    from_section := RTRIM(from_section, 'AND ');
                end loop;
                columns_null := ARRAY[]::text[];
            END IF;

            ret := row(columns_null, columns_no_null, ''::text, from_section);
        end if;

    ELSE--base table
        RAISE NOTICE 'LEAF';
        --leaf node
        execute(format('SELECT array_agg(tbl) FROM join_tree WHERE tname ~ ''%s''', root)) INTO tbl;
        RAISE NOTICE '%', tbl;
        --add column names
        --
        if lift THEN --generate cofactor matrix now
            -- have to fill nulls

            FOREACH col IN ARRAY tbl[1].columns_null LOOP
                columns_null_query := columns_null_query || col || format('tmp_imputed_%s.%s', col, col);
                from_section := from_section || format(' JOIN tmp_imputed_%s.%s ON ', col, col);

                RAISE NOTICE ' % ', tbl[1].columns_null;

                FOREACH col1 IN ARRAY (t_info->format('tmp_imputed_%s', col))::text[] LOOP--key of imputed table
                    from_section := from_section || format(' tmp_imputed_%s.%s = %s.%s AND ', col, col1, subpath(children[1], -1), col1);
                end loop;
                from_section := RTRIM(from_section, 'AND ');
            end loop;

            for j in 1..cardinality(key) loop--add keys to select (SELECT keys, cofactor...)
                IF new_key[j] IS NOT NULL THEN
                    cofactor_keys := cofactor_keys || format('%s AS %s, ', key[j], new_key[j]);
                    group_by := group_by || format('%s, ', key[j]);
                END IF;
            END LOOP;
            cofactor_keys := RTRIM(cofactor_keys, ', ');
            cofactor_query := cofactor_keys || ', SUM(' || format(generate_cofactor_query(cardinality(tbl[1].columns_null), cardinality(tbl[1].columns_no_null), -1), VARIADIC columns_null_query || tbl[1].columns_no_null )|| format(') AS cof_%s', tname);

            ---join table imputed values

            ret := (ARRAY[]::text[], ARRAY[]::text[], ARRAY[format('cof_%s', tname)], cofactor_query, format('%s AS %s %s GROUP BY %s', tbl[1].name, tname, from_section, RTRIM(group_by, ', ')));--, VARIADIC tbl[1].columns_no_null || tbl[1].columns_null);

        ELSE--generate cofactor in other points

            from_section := format('%s AS %s', tbl[1].name, tname);
            --could fill nulls

            IF fill_nulls THEN
                FOREACH col IN ARRAY tbl[1].columns_null LOOP
                    columns_no_null := columns_no_null || format(' coalesce(%s.%s , tmp_imputed_%s.%s) ', tname, col, col, col);
                    from_section := from_section || format(' JOIN tmp_imputed_%s.%s ON ', col, col);
                    FOREACH col1 IN ARRAY (t_info->format('tmp_imputed_%s', col))::text[] LOOP--key of imputed table
                        from_section := from_section || format(' tmp_imputed_%s.%s = %s.%s AND ', col, col1, tname, col1);
                    end loop;
                    from_section := RTRIM(from_section, 'AND ');
                end loop;
                tbl[1].columns_null := ARRAY[]::text[];
            END IF;

            for j in 1..cardinality(tbl[1].columns_null) loop--there are cols null here, join with imputed ...
                col := format('%s.%s', tname, tbl[1].columns_null[j]);
                --col := format(' coalesce(%s.%s, tmp_imputed_%s.%s) ', tname, tbl[1].columns_null[j], tbl[1].columns_null[j], tbl[1].columns_null[j]);
                tbl[1].columns_null[j] := col;
            end loop;

            ---join table imputed values

            for j in 1..cardinality(tbl[1].columns_no_null) loop
                col := format('%s.%s', tname, tbl[1].columns_no_null[j]);
                tbl[1].columns_no_null[j] := col;
            end loop;
            ret := (tbl[1].columns_null,  tbl[1].columns_no_null || columns_no_null, ARRAY[]::text[], ''::text, from_section);--, VARIADIC tbl[1].columns_no_null || tbl[1].columns_null);
        end if;
    END IF;

RAISE NOTICE 'RETURN %', ret;
return ret;
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
    END LOOP;

    --_triple := generate_full_cofactor(table_n, _schema || '.', no_id_columns_no_null, _nan_cols, _imputed_data, '');

    ---lift tables

    _triple := execute(generate_cofactor('locn', true, ARRAY[]::text[], ARRAY[]::text[]));

    for counter in 1..iterations
        loop
        FOREACH col IN ARRAY _nan_cols
            LOOP
            _start_ts := clock_timestamp();
            _triple_removal := execute(generate_cofactor('locn', true, ARRAY[]::text[], ARRAY[]::text[]));
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

            _triple := _triple_train + execute(generate_cofactor('locn', true, ARRAY[]::text[], ARRAY[]::text[]));
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
      --join_tree comp_tree;
      t_impute t_info;
      join_tables text[];
  BEGIN
      --define tables
    tbls := tbls || row('Inventory', 'retailer', '{"inventoryunits"}', '{}', '{"locn", "dateid", "ksn"}', '{"ksn"}')::t_info;
    tbls := tbls || row('Location', 'retailer', '{"zip","rgn_cd","clim_zn_nbr","tot_area_sq_ft","sell_area_sq_ft","avghhi","supertargetdistance","supertargetdrivetime","targetdistance","targetdrivetime","walmartdistance","walmartdrivetime","walmartsupercenterdistance","walmartsupercenterdrivetime"}', '{}', 'locn', '{"zip"}')::t_info;
    tbls := tbls || row('Weather', 'retailer', '{"rain","snow","maxtemp","mintemp","meanwind","thunder"}', '{}', '{"locn", "dateid"}', '{"locn"}')::t_info;
    tbls := tbls || row('Item', 'retailer', '{"subcategory","category","categoryCluster","prize"}', '{}', 'ksn', '{"ksn"}')::t_info;
    tbls := tbls || row('Census', 'retailer', '{"zip","population","white","asian","pacific","black","medianage","occupiedhouseunits","houseunits","families","households","husbwife","males","females","householdschildren","hispanic"}', '{}', 'zip', '{"zip"}')::t_info;
    join_tables := ARRAY['Inventory.locn = Location.locn', 'Inventory.locn = Weather.locn AND Inventory.dateid = Weather.dateid', 'Inventory.ksn = Retailer.ksn', 'Census.zip = Location.zip'];

    return impute(tbls, t_impute, join_tables,1);
  END;
$$ LANGUAGE plpgsql;


CREATE EXTENSION "ltree";
CREATE EXTENSION "hstore";

--tname: path name
--tbl: table info (if node is leaf)
--lift: lift all the attributes
--key: join key
CREATE TABLE join_tree (tname ltree, tbl t_info, lift boolean, key text[], new_key_name text[], fill_nulls boolean);
INSERT INTO join_tree VALUES ('locn', NULL, true, NULL, NULL, true);
INSERT INTO join_tree VALUES ('locn.zip', NULL, true, ARRAY['Location.locn'], ARRAY['locn'], true);--lifts Census
INSERT INTO join_tree VALUES ('locn.zip.Location', row('Location', 'retailer', '{"zip","rgn_cd","clim_zn_nbr","tot_area_sq_ft","sell_area_sq_ft","avghhi","supertargetdistance","supertargetdrivetime","targetdistance","targetdrivetime","walmartdistance","walmartdrivetime","walmartsupercenterdistance","walmartsupercenterdrivetime"}', '{}', '{"locn"}', '{"zip"}')::t_info, true, ARRAY['Location.zip', 'Location.locn'], ARRAY['zip', 'locn'], true);
INSERT INTO join_tree VALUES ('locn.zip.Census', row('Census', 'retailer', '{"zip","population","white","asian","pacific","black","medianage","occupiedhouseunits","houseunits","families","households","husbwife","males","females","householdschildren","hispanic"}', '{}', '{"zip"}', '{"zip"}')::t_info, false, ARRAY['Census.zip', NULL], ARRAY['zip', NULL], true);
INSERT INTO join_tree VALUES ('locn.dateid', NULL, false, ARRAY['ksn.locn'], ARRAY['locn'], true);
INSERT INTO join_tree VALUES ('locn.dateid.ksn', NULL, true, ARRAY['Inventory.dateid', 'Inventory.locn'], ARRAY['dateid', 'locn'], true);
INSERT INTO join_tree VALUES ('locn.dateid.ksn.Inventory', row('Inventory', 'retailer', '{"inventoryunits"}', '{}', '{"locn", "dateid", "ksn"}', '{"ksn"}')::t_info,false, ARRAY['Inventory.dateid', 'Inventory.locn', 'Inventory.ksn'], ARRAY['ksn','dateid', 'locn'], true);
INSERT INTO join_tree VALUES ('locn.dateid.ksn.Item', row('Item', 'retailer', '{"categoryCluster","prize"}', '{"subcategory","category"}', '{"ksn"}', '{"ksn"}')::t_info, true, ARRAY['Item.ksn', 'Item.dateid', 'Item.locn'], ARRAY['ksn', NULL, NULL], true);
INSERT INTO join_tree VALUES ('locn.dateid.Weather', row('Weather', 'retailer', '{"rain","snow","maxtemp","mintemp","meanwind","thunder"}', '{}', '{"locn", "dateid"}', '{"locn"}')::t_info, false, ARRAY['Weather.dateid', 'Weather.locn'], ARRAY['dateid', 'locn'], true);

CREATE INDEX path_gist_idx ON join_tree USING gist(tname);
CREATE INDEX path_idx ON join_tree USING btree(tname);

select generate_cofactor('locn.dateid.ksn', true, ARRAY[]::text[], ARRAY[]::text[], true, hstore(ARRAY['tmp_imputed_subcategory','tmp_imputed_category'], ARRAY['{"ksn1","ksn2"}','{"ksn1","ksn2"}']));