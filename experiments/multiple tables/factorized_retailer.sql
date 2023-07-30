CREATE OR REPLACE PROCEDURE MICE_factorized(
    input_table_name text,
    output_table_name text,
    continuous_columns text[],
    continuous_columns_fact text[],
    categorical_columns text[],--make sure this follows the order of the cofactor matrix!
    categorical_columns_fact text[],--make sure this follows the order of the cofactor matrix!
    continuous_columns_null text[],
    categorical_columns_null text[]
)
    LANGUAGE plpgsql AS
$$
DECLARE
    start_ts                     timestamptz;
    end_ts                       timestamptz;
    query                        text;
    query2                       text;
    subquery                     text;
    query_null1                  text;
    query_null2                  text;
    col_averages                 float8[];
    col_mode                     int4[];
    tmp_array                    text[];
    tmp_array2                   text[];
    tmp_array3                   text[];
    cofactor_global              cofactor;
    cofactor_null                cofactor;
    col                          text;
    params                       float8[];
    label_index                  int4;
    max_range                    int4;
    tmp_columns_names			text[];
    columns_lower text[];
	categorical_uniq_vals_sorted int[] := ARRAY[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,23,24,25,26,27,28, 29,30,31,32,3,14,15,31,32,33,35,40,64,65,48,102,104,105,106,107,108,109,0,1,0,1, 0, 1];--categorical columns sorted by values -> RETAILER
    upper_bound_categorical  int[] := ARRAY[31,41,49,51,53, 55];--5,9,11 
    low_bound_categorical  int[]; 

    --categorical_uniq_vals_sorted int[] := ARRAY [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22, 0,1,0,1,0,1];--categorical columns sorted by values (cat, columns order)---0,1,3,5,9,1,2,3,4,1,2
    --upper_bound_categorical      int[] := ARRAY [23,25,27,29];--5,9,11

BEGIN
    -- COMPUTE COLUMN AVERAGES (over a subset)
    SELECT array_agg('AVG(' || x || ')')
    FROM unnest(continuous_columns_null) AS x
    INTO tmp_array;

    SELECT array_agg('MODE() WITHIN GROUP (ORDER BY ' || x || ') ')
    FROM unnest(categorical_columns_null) AS x
    INTO tmp_array2;

    query := ' SELECT ARRAY[ ' || array_to_string(tmp_array, ', ') || ' ]::float8[]' ||
             ' FROM ( SELECT ' || array_to_string(continuous_columns_null, ', ') ||
             ' FROM ' || input_table_name || ' LIMIT 500000 ) AS t';
    query2 := ' SELECT ARRAY[ ' || array_to_string(tmp_array2, ', ') || ' ]::int[]' ||
              ' FROM ( SELECT ' || array_to_string(categorical_columns_null, ', ') ||
              ' FROM ' || input_table_name || ' LIMIT 500000 ) AS t';

    RAISE DEBUG '%', query;

    start_ts := clock_timestamp();
    IF array_length(continuous_columns_null, 1) > 0 THEN
        EXECUTE query INTO col_averages;
    end if;

    IF array_length(categorical_columns_null, 1) > 0 THEN
        EXECUTE query2 INTO col_mode;
    end if;
    end_ts := clock_timestamp();


    RAISE DEBUG 'AVERAGES: %', col_averages;
    RAISE INFO 'COMPUTE COLUMN AVERAGES: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

    -- CREATE TABLE WITH MISSING VALUES
    query := 'DROP TABLE IF EXISTS ' || output_table_name;
    RAISE DEBUG '%', query;
    EXECUTE QUERY;

    SELECT (SELECT array_agg(x || ' float8')
            FROM unnest(continuous_columns_fact) AS x) ||
           (SELECT array_agg(x || ' int')
            FROM unnest(categorical_columns_fact) AS x) ||
           (SELECT array_agg(x || '_ISNULL bool')
            FROM unnest(continuous_columns_null) AS x) ||
           (SELECT array_agg(x || '_ISNULL bool')
            FROM unnest(categorical_columns_null) AS x)
               || (
               ARRAY [ 'NULL_CNT int', 'NULL_COL_ID int', 'ROW_ID serial' ]
               )
    INTO tmp_array;

    query := 'CREATE UNLOGGED TABLE ' || output_table_name || 
    '(locn integer, dateid integer, ksn integer, inventoryunits float8, inventoryunits_ISNULL bool, NULL_CNT int, NULL_COL_ID int, ROW_ID serial) PARTITION BY RANGE (NULL_CNT)';
    RAISE DEBUG '%', query;
    EXECUTE QUERY;--update here

    query := 'CREATE UNLOGGED TABLE ' || output_table_name || '_nullcnt0' ||
             ' PARTITION OF ' || output_table_name ||
             ' FOR VALUES FROM (0) TO (1) WITH (fillfactor=100)';
    RAISE DEBUG '%', query;
    EXECUTE QUERY;

    query := 'CREATE UNLOGGED TABLE ' || output_table_name || '_nullcnt1' ||
             ' PARTITION OF ' || output_table_name ||
             ' FOR VALUES FROM (1) TO (2) PARTITION BY RANGE (NULL_COL_ID)';
    RAISE DEBUG '%', query;
    EXECUTE QUERY;

    if array_length(categorical_columns_null, 1) > 0 then
        max_range := array_length(continuous_columns_null, 1) + array_length(categorical_columns_null, 1);
    else
        max_range := array_length(continuous_columns_null, 1);
    end if;

    FOR col_id in 1..max_range
        LOOP
            query := 'CREATE UNLOGGED TABLE ' || output_table_name || '_nullcnt1_col' || col_id ||
                     ' PARTITION OF ' || output_table_name || '_nullcnt1' ||
                     ' FOR VALUES FROM (' || col_id || ') TO (' || col_id + 1 || ') WITH (fillfactor=100)';
            RAISE DEBUG '%', query;
            EXECUTE QUERY;
        END LOOP;


    IF max_range >= 2 THEN
        if array_length(categorical_columns_null, 1) > 0 then
    		query := 'CREATE UNLOGGED TABLE ' || output_table_name || '_nullcnt2' || 
             ' PARTITION OF ' || output_table_name ||
             ' FOR VALUES FROM (2) TO (' || array_length(continuous_columns_null, 1) + array_length(categorical_columns_null, 1) + 1 || ') WITH (fillfactor=75)';
        else
            query := 'CREATE UNLOGGED TABLE ' || output_table_name || '_nullcnt2' || 
            ' PARTITION OF ' || output_table_name ||
            ' FOR VALUES FROM (2) TO (' || array_length(continuous_columns_null, 1) + 1 || ') WITH (fillfactor=75)';
        end if;    
        RAISE DEBUG '%', query;
        EXECUTE QUERY;
    end if;

    COMMIT;

    -- INSERT INTO TABLE WITH MISSING VALUES
    start_ts := clock_timestamp();
    SELECT (SELECT array_agg(
                           CASE
                               WHEN array_position(continuous_columns_null, x) IS NULL THEN
                                   x
                               ELSE
                                    'COALESCE(' || x || ', ' ||
                                    col_averages[array_position(continuous_columns_null, x)] || ')'
                               END
                       )
            FROM unnest(continuous_columns_fact) AS x) ||
           (SELECT array_agg(
                            CASE
                                WHEN array_position(categorical_columns_null, x) IS NULL THEN
                                    x
                                ELSE
                                    'COALESCE(' || x || ', ' ||
                                    col_mode[array_position(categorical_columns_null, x)] || ')'
                                END
                            )
            FROM unnest(categorical_columns_fact) AS x) ||
           (SELECT array_agg(x || ' IS NULL')
            FROM unnest(continuous_columns_null) AS x) || (SELECT array_agg(x || ' IS NULL')
                                                           FROM unnest(categorical_columns_null) AS x) ||
           (SELECT ARRAY [ array_to_string(array_agg('(CASE WHEN ' || x || ' IS NULL THEN 1 ELSE 0 END)'), ' + ') ]
            FROM unnest(continuous_columns_null || categorical_columns_null) AS x)
    INTO tmp_array;

    SELECT (SELECT array_agg(
                           CASE
                               WHEN array_position(continuous_columns_null, x) IS NULL THEN
                                               ' WHEN ' || x || ' IS NULL THEN ' ||
                                               array_position(categorical_columns_null, x) +
                                               array_length(continuous_columns_null, 1)
                               ELSE
                                               ' WHEN ' || x || ' IS NULL THEN ' ||
                                               array_position(continuous_columns_null, x)
                               END
                       )
            FROM unnest(continuous_columns_null || categorical_columns_null) AS x)
    INTO tmp_array2;


    query := 'INSERT INTO ' || output_table_name ||
             ' SELECT ' || array_to_string(tmp_array, ', ') || ', CASE ' || array_to_string(tmp_array2, '  ') ||
             ' ELSE 0 END ' ||
             ' FROM ' || input_table_name;
    RAISE DEBUG '%', query;

    EXECUTE query;
    end_ts := clock_timestamp();
    RAISE INFO 'INSERT INTO TABLE WITH MISSING VALUES: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

    -- COMPUTE MAIN COFACTOR
    query := 'SELECT
  SUM(t2.cnt * t3.cnt)
FROM
  (
    SELECT
      t1.locn, SUM(
        t1.cnt * to_cofactor(ARRAY[W.maxtemp,
          W.mintemp, W.meanwind], ARRAY[W.rain, W.snow,
          W.thunder] :: int4[]
        )
      ) AS cnt
    FROM
      (SELECT
          Inv.locn,
          Inv.dateid,SUM(
            to_cofactor(ARRAY[Inv.inventoryunits,
              It.prize], ARRAY[It.subcategory,
              It.category, It.categoryCluster]:: int4[]
            )
          ) AS cnt
        FROM '
          ||output_table_name || ' AS Inv
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
          L.walmartdistance, L.walmartdrivetime,  L.walmartsupercenterdistance, walmartsupercenterdrivetime],ARRAY[] :: int4[]
        )
      ) AS cnt
    FROM
      retailer.Location AS L
      JOIN retailer.Census AS C ON L.zip = C.zip
    GROUP BY
      L.locn
  ) AS t3 ON t2.locn = t3.locn;';

    RAISE DEBUG '%', query;

    start_ts := clock_timestamp();
    EXECUTE query INTO STRICT cofactor_global;
    end_ts := clock_timestamp();
    RAISE INFO 'COFACTOR global: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

    COMMIT;

    FOR i in 1..1
        LOOP
            RAISE INFO 'Iteration %', i;
            
            FOREACH col in ARRAY continuous_columns_null
                LOOP
                    RAISE INFO '  |- Column %', col;

                    -- COMPUTE COFACTOR WHERE $col IS NULL

                    query_null1 := 'SELECT
  SUM(t2.cnt * t3.cnt)
FROM
  (
    SELECT
      t1.locn, SUM(
        t1.cnt * to_cofactor(ARRAY[W.maxtemp,
          W.mintemp, W.meanwind], ARRAY[W.rain, W.snow,
          W.thunder] :: int4[]
        )
      ) AS cnt
    FROM
      (SELECT
          Inv.locn,
          Inv.dateid,SUM(
            to_cofactor(ARRAY[Inv.inventoryunits,
              It.prize], ARRAY[It.subcategory,
              It.category, It.categoryCluster]:: int4[]
            )
          ) AS cnt
        FROM '|| output_table_name ||' AS Inv
          JOIN retailer.Item AS It ON Inv.ksn = It.ksn  ' ||
        'WHERE (NULL_CNT = 1 AND NULL_COL_ID = ' || array_position(continuous_columns_null, col) ||
        ' ) GROUP BY
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
          L.walmartdistance, L.walmartdrivetime,  L.walmartsupercenterdistance, walmartsupercenterdrivetime],ARRAY[] :: int4[]
        )
      ) AS cnt
    FROM
      retailer.Location AS L
      JOIN retailer.Census AS C ON L.zip = C.zip
    GROUP BY
      L.locn
  ) AS t3 ON t2.locn = t3.locn;';


                    RAISE DEBUG '%', query_null1;

                    start_ts := clock_timestamp();
                    EXECUTE query_null1 INTO STRICT cofactor_null;
                    end_ts := clock_timestamp();
                    RAISE INFO 'COFACTOR NULL (1): ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

                    IF cofactor_null IS NOT NULL THEN
                        cofactor_global := cofactor_global - cofactor_null;
                    END IF;

                    query_null2 := 'SELECT
  SUM(t2.cnt * t3.cnt)
FROM
  (
    SELECT
      t1.locn, SUM(
        t1.cnt * to_cofactor(ARRAY[W.maxtemp,
          W.mintemp, W.meanwind], ARRAY[W.rain, W.snow,
          W.thunder] :: int4[]
        )
      ) AS cnt
    FROM
      (SELECT
          Inv.locn,
          Inv.dateid,SUM(
            to_cofactor(ARRAY[Inv.inventoryunits,
              It.prize], ARRAY[It.subcategory,
              It.category, It.categoryCluster]:: int4[]
            )
          ) AS cnt
        FROM  '|| output_table_name ||' AS Inv
          JOIN retailer.Item AS It ON Inv.ksn = It.ksn  ' ||
        'WHERE NULL_CNT >= 2 AND ' || col || '_ISNULL ' ||
        ' GROUP BY
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
          L.walmartdistance, L.walmartdrivetime,  L.walmartsupercenterdistance, walmartsupercenterdrivetime],ARRAY[] :: int4[]
        )
      ) AS cnt
    FROM
      retailer.Location AS L
      JOIN retailer.Census AS C ON L.zip = C.zip
    GROUP BY
      L.locn
  ) AS t3 ON t2.locn = t3.locn;';


                    RAISE DEBUG '%', query_null2;

                    start_ts := clock_timestamp();
                    EXECUTE query_null2 INTO STRICT cofactor_null;
                    end_ts := clock_timestamp();
                    RAISE INFO 'COFACTOR NULL (2): ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

                    IF cofactor_null IS NOT NULL THEN
                        cofactor_global := cofactor_global - cofactor_null;
                    END IF;

                    -- TRAIN
                    label_index := array_position(continuous_columns, col);
                    start_ts := clock_timestamp();
                    params := ridge_linear_regression(cofactor_global, label_index - 1, 0.001, 0, 10000, 1);
                    end_ts := clock_timestamp();
                    RAISE INFO 'TRAIN: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
                    RAISE DEBUG '%', params;
                    RAISE DEBUG '%', label_index;
                    
                    --impute                    
                    query := 'SELECT array_agg(quote_ident(column_name)) FROM information_schema.columns WHERE table_name = ''' || output_table_name || ''';';
            		EXECUTE query INTO tmp_columns_names;
            		RAISE DEBUG '%', tmp_columns_names;
            		
            		-- IMPUTE
                    SELECT array_agg(x || ' * ' || params[array_position(continuous_columns, x) + 1])
                    FROM unnest(continuous_columns) AS x
                    WHERE array_position(continuous_columns, x) != label_index
                    INTO tmp_array;

                    
                    low_bound_categorical := ARRAY[0] || array_remove(upper_bound_categorical , upper_bound_categorical[array_upper(upper_bound_categorical, 1)]);
                        
            		SELECT array_agg('(ARRAY['||array_to_string(params[(array_length(continuous_columns, 1) + a.bound_down + 1 + 1) : (array_length(continuous_columns, 1) + a.bound_up + 1)], ', ')||'])[array_position(ARRAY['||array_to_string(categorical_uniq_vals_sorted[bound_down+1:bound_up], ', ') ||'], '||x||')]')
            		FROM unnest(categorical_columns, upper_bound_categorical, low_bound_categorical) WITH ORDINALITY a(x, bound_up, bound_down, nr) 
            		INTO tmp_array2;--add categorical to imputation

                    SELECT array_agg(LOWER(x)) FROM unnest(continuous_columns) as x INTO columns_lower;
                    SELECT array_agg(
                    	CASE WHEN x = 'inventoryunits' THEN
                    		array_to_string(array_append(array_prepend(params[1]::text, tmp_array || tmp_array2), '(sqrt(-2 * ln(random()))*cos(2*pi()*random()))*'||sqrt(params[array_length(params, 1)])::text), ' + ') || ' AS ' || x
                		ELSE
                    		x || ' AS ' || x
                		END
                		)
            		FROM unnest(tmp_columns_names) AS x
            		INTO tmp_array3;

                    
                    start_ts := clock_timestamp();

                    query := 'ALTER TABLE '||output_table_name || '_nullcnt1 DETACH PARTITION '|| output_table_name ||'_nullcnt1_col'||array_position(continuous_columns_null, col);
                    RAISE DEBUG '%', query;
                    EXECUTE query;
                    query := 'ALTER TABLE '||output_table_name ||'_nullcnt1_col'||array_position(continuous_columns_null, col) ||' RENAME TO tmp_table';
                    RAISE DEBUG '%', query;
                    EXECUTE query;
                    query := 'CREATE UNLOGGED TABLE ' || output_table_name || '_nullcnt1_col'||array_position(continuous_columns_null, col) || ' AS ';
                    query := query || ' SELECT ' || array_to_string(tmp_array3, ' , ') || ' FROM ';
                    query := query || '(SELECT * FROM (SELECT * FROM tmp_table as Inv JOIN retailer.Item AS It USING(ksn)) as t1 JOIN retailer.Weather AS W USING(locn,dateid )) AS t2
                                        JOIN (SELECT L.locn, C.population, C.white, C.asian, C.pacific, C.black, C.medianage,  C.occupiedhouseunits, C.houseunits, C.families, C.households,C.husbwife,
                                             C.males, C.females, C.householdschildren, C.hispanic,  L.rgn_cd, L.clim_zn_nbr, L.tot_area_sq_ft, L.sell_area_sq_ft, L.avghhi, L.supertargetdistance,
                                             L.supertargetdrivetime, L.targetdistance,L.targetdrivetime,
                                             L.walmartdistance, L.walmartdrivetime,  L.walmartsupercenterdistance, L.walmartsupercenterdrivetime
                                             FROM retailer.Location AS L JOIN retailer.Census AS C USING(zip)) AS t3 USING(locn)';

                    
                    RAISE DEBUG '%', query;
                    EXECUTE query;
                    EXECUTE 'ALTER TABLE '|| output_table_name || '_nullcnt1_col'||array_position(continuous_columns_null, col) ||' ALTER COLUMN row_id SET NOT NULL';
                    query := 'ALTER TABLE '|| output_table_name || '_nullcnt1 ATTACH PARTITION '||output_table_name || '_nullcnt1_col'||array_position(continuous_columns_null, col) ||' FOR VALUES FROM (' || array_position(continuous_columns_null, col) || ') TO (' || array_position(continuous_columns_null, col) + 1 || ')';
                    RAISE DEBUG '%', query;
                    EXECUTE query;
                    EXECUTE 'DROP TABLE tmp_table';
                    
                    end_ts := clock_timestamp();
                    RAISE INFO 'IMPUTE DATA (1): ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
                    
                    
                    start_ts := clock_timestamp();
                    EXECUTE query_null1 INTO STRICT cofactor_null;
                    end_ts := clock_timestamp();
                    RAISE INFO 'COFACTOR NULL (1): ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

                    IF cofactor_null IS NOT NULL THEN
                        cofactor_global := cofactor_global + cofactor_null;
                    END IF;

                    start_ts := clock_timestamp();
                    EXECUTE query_null2 INTO STRICT cofactor_null;
                    end_ts := clock_timestamp();
                    RAISE INFO 'COFACTOR NULL (2): ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

                    IF cofactor_null IS NOT NULL THEN
                        cofactor_global := cofactor_global + cofactor_null;
                    END IF;

                    COMMIT;

                END LOOP;
        END LOOP;
END
$$;

CALL MICE_factorized('retailer.inventory', 'output_inventory',
                     ARRAY ['inventoryunits', 'prize', 'maxtemp', 'mintemp', 'meanwind', 'population', 'white', 'asian', 'pacific', 'black', 'medianage', 'occupiedhouseunits', 'houseunits', 'families', 'households', 'husbwife', 'males', 'females', 'householdschildren', 'hispanic', 'rgn_cd', 'clim_zn_nbr', 'tot_area_sq_ft', 'sell_area_sq_ft', 'avghhi', 'supertargetdistance', 'supertargetdrivetime', 'targetdistance', 'targetdrivetime', 'walmartdistance', 'walmartdrivetime', 'walmartsupercenterdistance', 'walmartsupercenterdrivetime'],
                     ARRAY ['locn','dateid','ksn','inventoryunits']::text[],
                     ARRAY ['subcategory', 'category', 'categoryCluster','rain', 'snow', 'thunder'],
                     ARRAY []::text[],
                    ARRAY ['inventoryunits']::text[],
                    ARRAY []::text[]
                        );