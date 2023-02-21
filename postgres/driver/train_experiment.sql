CREATE OR REPLACE PROCEDURE train_madlib_retailer(
        continuous_columns text[],
        categorical_columns text[]
    ) LANGUAGE plpgsql AS $$
DECLARE
    query text;
    tbl text;
    cat_columns_names text[];
    start_ts timestamptz;
    end_ts   timestamptz;

BEGIN
    -- COMPUTE COLUMN AVERAGES (over a subset)
    query := 'DROP TABLE IF EXISTS tbl_madlib_retailer';
    EXECUTE query;

    query := 'CREATE UNLOGGED TABLE tbl_madlib_retailer AS ' ||
             '(SELECT * FROM retailer.Weather ' ||
             'JOIN retailer.Location USING (locn) ' ||
             'JOIN retailer.Inventory USING (locn, dateid) ' ||
             'JOIN retailer.Item USING (ksn) ' ||
             'JOIN retailer.Census USING (zip))';

    RAISE NOTICE '%', query;
    start_ts := clock_timestamp();
    EXECUTE query;
    end_ts := clock_timestamp();
    RAISE INFO 'TIME JOIN: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));


    tbl := 'tbl_madlib_retailer';

    --if categorical values
    IF array_length(categorical_columns, 1) > 0 THEN
        query := 'SELECT madlib.encode_categorical_variables(''tbl_madlib_retailer'', ''tbl_madlib_retailer_1_hot'',''' ||
                array_to_string(categorical_columns, ', ') || ''');';
        RAISE NOTICE '%', query;
        start_ts := clock_timestamp();
        EXECUTE query;
        end_ts := clock_timestamp();
        RAISE INFO 'TIME 1-hot: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
        tbl := 'tbl_madlib_retailer_1_hot';

        query := 'SELECT array_agg(quote_ident(column_name)) FROM information_schema.columns WHERE table_name = ''tbl_madlib_retailer_1_hot''';
        RAISE NOTICE '%', query;
        EXECUTE query INTO cat_columns_names;
        continuous_columns := cat_columns_names;
    end if;
    ---continuous_columns has also 1-hot encoded feats
    query := 'SELECT madlib.linregr_train( ''' || tbl || ''', ''res_linregr'' ,''' ||
                             'inventoryunits' || ''', ''ARRAY[' || array_to_string(continuous_columns, ', ') || ']'')';

    RAISE NOTICE '%', query;
    start_ts := clock_timestamp();
    EXECUTE query;
    end_ts := clock_timestamp();
    RAISE INFO 'TIME train: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

    IF array_length(categorical_columns, 1) > 0 THEN
        EXECUTE 'DROP TABLE tbl_madlib_retailer_1_hot';
    end if;
    EXECUTE 'DROP TABLE tbl_madlib_retailer';
    EXECUTE 'DROP TABLE res_linregr';
END$$;

CALL train_madlib_retailer(ARRAY['population', 'white', 'asian', 'pacific', 'black', 'medianage', 'occupiedhouseunits', 'houseunits', 'families', 'households', 'husbwife', 'males', 'females', 'householdschildren', 'hispanic', 'rgn_cd', 'clim_zn_nbr', 'tot_area_sq_ft', 'sell_area_sq_ft', 'avghhi', 'supertargetdistance', 'supertargetdrivetime', 'targetdistance', 'targetdrivetime', 'walmartdistance', 'walmartdrivetime', 'walmartsupercenterdistance', 'walmartsupercenterdrivetime' , 'prize', 'feat_1', 'maxtemp', 'mintemp', 'meanwind', 'thunder', 'inventoryunits'], ARRAY['subcategory', 'category', 'categoryCluster', 'rain', 'snow']::text[]);

CREATE OR REPLACE PROCEDURE train_postgres_retailer(
        continuous_columns text[],
        categorical_columns text[]
    ) LANGUAGE plpgsql AS $$
DECLARE
    query text;
    tbl text;
    cat_columns_names text[];
    start_ts timestamptz;
    end_ts   timestamptz;
    i int;
    j int;
    values float4[];
    params float8[];
BEGIN
    -- COMPUTE COLUMN AVERAGES (over a subset)
    query := 'DROP TABLE IF EXISTS tbl_postgres_retailer';
    EXECUTE query;

    query := 'CREATE UNLOGGED TABLE tbl_postgres_retailer AS ' ||
             '(SELECT * FROM retailer.Weather ' ||
             'JOIN retailer.Location USING (locn) ' ||
             'JOIN retailer.Inventory USING (locn, dateid) ' ||
             'JOIN retailer.Item USING (ksn) ' ||
             'JOIN retailer.Census USING (zip))';

    RAISE NOTICE '%', query;
    start_ts := clock_timestamp();
    EXECUTE query;
    end_ts := clock_timestamp();
    RAISE INFO 'TIME JOIN: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));


    tbl := 'tbl_postgres_retailer';

    --if categorical values
    IF array_length(categorical_columns, 1) > 0 THEN
        query := 'SELECT madlib.encode_categorical_variables(''tbl_postgres_retailer'', ''tbl_postgres_retailer_1_hot'', ''' ||
                   array_to_string(categorical_columns, ', ') || ''');';
        RAISE NOTICE '%', query;
        start_ts := clock_timestamp();
        EXECUTE query;
        end_ts := clock_timestamp();
        RAISE INFO 'TIME 1-hot: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
        tbl := 'tbl_postgres_retailer_1_hot';

        query := 'SELECT array_agg(column_name) FROM information_schema.columns WHERE table_name = ''tbl_postgres_retailer_1_hot''';
        RAISE NOTICE '%', query;
        EXECUTE query INTO cat_columns_names;
        continuous_columns := cat_columns_names;
    end if;

    RAISE NOTICE '%', continuous_columns;
    continuous_columns := array_prepend('1', continuous_columns);
    RAISE NOTICE '%', continuous_columns;

    query := '';
    FOR i in 1..array_length(continuous_columns, 1) LOOP
        FOR j in i..array_length(continuous_columns, 1) LOOP
            query := query || ' SUM(' || continuous_columns[i] || '*' || continuous_columns[j] || '), ';
        end loop;
    end loop;

    query := 'SELECT ARRAY[' || rtrim(query, ', ') || '] FROM tbl_postgres_retailer';
    RAISE NOTICE '%', query;
    start_ts := clock_timestamp();
    EXECUTE query INTO values;
    RAISE NOTICE '%', values;
    end_ts := clock_timestamp();
    RAISE INFO 'TIME aggregates: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

    start_ts := clock_timestamp();
    params := ridge_linear_regression_from_params(values, 0, 0.001, 0, 10000);
    end_ts := clock_timestamp();
    RAISE INFO 'TIME train: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
END$$;
--'subcategory', 'category', 'categoryCluster', 'rain', 'snow'
CALL train_postgres_retailer(ARRAY['population', 'white', 'asian', 'pacific', 'black', 'medianage', 'occupiedhouseunits', 'houseunits', 'families', 'households', 'husbwife', 'males', 'females', 'householdschildren', 'hispanic', 'rgn_cd', 'clim_zn_nbr', 'tot_area_sq_ft', 'sell_area_sq_ft', 'avghhi', 'supertargetdistance', 'supertargetdrivetime', 'targetdistance', 'targetdrivetime', 'walmartdistance', 'walmartdrivetime', 'walmartsupercenterdistance', 'walmartsupercenterdrivetime' , 'prize', 'feat_1', 'maxtemp', 'mintemp', 'meanwind', 'thunder', 'inventoryunits'], ARRAY[]::text[]);

CREATE OR REPLACE PROCEDURE train_cofactor_join_retailer(
        continuous_columns text[],
        categorical_columns text[]
    ) LANGUAGE plpgsql AS $$
DECLARE
    query text;
    tbl text;
    cat_columns_names text[];
    start_ts timestamptz;
    end_ts   timestamptz;
    i int;
    j int;
    cofactor cofactor;
    label_index int;
    params float[];
BEGIN
    -- COMPUTE COLUMN AVERAGES (over a subset)
    query := 'DROP TABLE IF EXISTS tbl_postgres_retailer';
    EXECUTE query;

    query := 'CREATE UNLOGGED TABLE tbl_postgres_retailer AS ' ||
             '(SELECT * FROM retailer.Weather ' ||
             'JOIN retailer.Location USING (locn) ' ||
             'JOIN retailer.Inventory USING (locn, dateid) ' ||
             'JOIN retailer.Item USING (ksn) ' ||
             'JOIN retailer.Census USING (zip))';

    RAISE NOTICE '%', query;
    start_ts := clock_timestamp();
    EXECUTE query;
    end_ts := clock_timestamp();
    RAISE INFO 'TIME JOIN: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));


    tbl := 'tbl_postgres_retailer';

    query := 'SELECT SUM(to_cofactor(ARRAY[' || array_to_string(continuous_columns, ', ') || ']::float8[], ARRAY[' ||
             array_to_string(categorical_columns, ', ') || ']::int4[])) FROM tbl_postgres_retailer';

    RAISE NOTICE '%', query;
    start_ts := clock_timestamp();
    EXECUTE query INTO cofactor;
    end_ts := clock_timestamp();
    RAISE INFO 'TIME cofactor: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

    label_index := array_position(continuous_columns, 'inventoryunits');
    start_ts := clock_timestamp();
    params := ridge_linear_regression(cofactor, label_index - 1, 0.001, 0, 10000);
    end_ts := clock_timestamp();
    RAISE INFO 'TIME train: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

END$$;

CALL train_cofactor_join_retailer(ARRAY['population', 'white', 'asian', 'pacific', 'black', 'medianage', 'occupiedhouseunits', 'houseunits', 'families', 'households', 'husbwife', 'males', 'females', 'householdschildren', 'hispanic', 'rgn_cd', 'clim_zn_nbr', 'tot_area_sq_ft', 'sell_area_sq_ft', 'avghhi', 'supertargetdistance', 'supertargetdrivetime', 'targetdistance', 'targetdrivetime', 'walmartdistance', 'walmartdrivetime', 'walmartsupercenterdistance', 'walmartsupercenterdrivetime' , 'prize', 'feat_1', 'maxtemp', 'mintemp', 'meanwind', 'thunder', 'inventoryunits'], ARRAY['subcategory', 'category', 'categoryCluster', 'rain', 'snow']::text[])



CREATE OR REPLACE PROCEDURE train_cofactor_factorized_retailer() LANGUAGE plpgsql AS $$
DECLARE
    query text;
    tbl text;
    cat_columns_names text[];
    start_ts timestamptz;
    end_ts   timestamptz;
    i int;
    j int;
    cofactor cofactor;
    label_index int;
    params float[];
BEGIN
    query := 'SELECT
  SUM(t2.cnt * t3.cnt)
FROM
  (
    SELECT
      t1.locn, SUM(
        t1.cnt * to_cofactor(ARRAY[W.maxtemp,
          W.mintemp, W.meanwind, W.rain, W.snow,
          W.thunder], ARRAY[] :: int4[]
        )
      ) AS cnt
    FROM
      (SELECT
          Inv.locn,
          Inv.dateid,SUM(
            to_cofactor(ARRAY[Inv.inventoryunits,
              It.prize], ARRAY[It.subcategory,
              It.category, It.categoryCluster]
            )
          ) AS cnt
        FROM
          retailer.Inventory AS Inv
          JOIN retailer.Item AS It ON Inv.ksn = It.ksn
        GROUP BY
          Inv.locn,
          Inv.dateid
      ) AS t1
      JOIN retailer.Weather AS W ON t1.locn = W.locn
      AND t1.dateid = W.dateid
    GROUP BY
      t1.locn
  ) AS t2
  JOIN (
    SELECT
      L.locn, SUM(
        to_cofactor(ARRAY[C.population,
          C.white, C.asian, C.pacific, C.black,
          C.medianage,  C.occupiedhouseunits,
          C.houseunits, C.families, C.households,C.husbwife,
          C.males, C.females, C.householdschildren,
          C.hispanic,  L.rgn_cd,
          L.clim_zn_nbr, L.tot_area_sq_ft,
          L.sell_area_sq_ft, L.avghhi, L.supertargetdistance,
          L.supertargetdrivetime, L.targetdistance,L.targetdrivetime,
          L.walmartdistance, L.walmartdrivetime,  L.walmartsupercenterdistance],ARRAY[] :: int4[]
        )
      ) AS cnt
    FROM
      retailer.Location AS L
      JOIN retailer.Census AS C ON L.zip = C.zip
    GROUP BY
      L.locn
  ) AS t3 ON t2.locn = t3.locn;';

    --RAISE NOTICE '%', query;
    start_ts := clock_timestamp();
    EXECUTE query INTO cofactor;
    end_ts := clock_timestamp();
    RAISE INFO 'TIME cofactor: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
    label_index := 0;
    start_ts := clock_timestamp();
    params := ridge_linear_regression(cofactor, label_index, 0.001, 0, 10000);
    end_ts := clock_timestamp();
    RAISE INFO 'TIME train: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
END$$;

CALL train_cofactor_factorized_retailer();
