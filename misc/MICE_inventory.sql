CREATE SCHEMA test;

SET search_path TO "$user", public, test;
SET work_mem='512MB';

SELECT setseed(0.5);

DROP TABLE IF EXISTS retailer.minventory;
CREATE TABLE retailer.minventory(LIKE retailer.inventory INCLUDING ALL, id SERIAL);
INSERT INTO retailer.minventory
SELECT (CASE WHEN t.locn_rnd <= 0.05 THEN NULL ELSE t.locn END) AS locn,
       (CASE WHEN t.dateid_rnd <= 0.05 THEN NULL ELSE t.dateid END) AS dateid,
       t.ksn,
       t.inventoryunits
FROM (
    SELECT *,
           random() AS locn_rnd, 
           random() AS dateid_rnd
    FROM retailer.inventory
) t;

DO $$
DECLARE
    avg_locn FLOAT;
    avg_dateid FLOAT;
    cofactor cofactor;
    cofactor_locn cofactor;
    cofactor_locn_new cofactor;
    cofactor_dateid cofactor;
    cofactor_dateid_new cofactor;
    params FLOAT[];
    start_ts timestamptz;
    end_ts timestamptz;
    start_ts2 timestamptz;
    end_ts2 timestamptz;
BEGIN

    -- compute initial imputation values and cofactor matrix

    start_ts := clock_timestamp();
    start_ts2 := start_ts;
    SELECT AVG(CASE WHEN locn IS NOT NULL THEN locn END), 
           AVG(CASE WHEN dateid IS NOT NULL THEN dateid END)
           INTO STRICT avg_locn, avg_dateid
    FROM retailer.minventory;
    end_ts := clock_timestamp();
    RAISE NOTICE '[Init] AVG values: %' , 1000 * (extract(epoch FROM end_ts - start_ts));


    start_ts := clock_timestamp();
    SELECT SUM(cont_to_cofactor(coalesce(locn, avg_locn))* 
                cont_to_cofactor(coalesce(dateid, avg_dateid))* 
                cont_to_cofactor(ksn)*
                cont_to_cofactor(inventoryunits)
           )
           INTO STRICT cofactor
    FROM retailer.minventory;
    end_ts := clock_timestamp();
    RAISE NOTICE '[Init] Full cofactor matrix: %' , 1000 * (extract(epoch FROM end_ts - start_ts));


    -- create tables with missing values 

    start_ts := clock_timestamp();
    DROP TABLE IF EXISTS tmp_minventory_locn;
    CREATE TEMPORARY TABLE tmp_minventory_locn AS
    SELECT dateid, ksn, inventoryunits, id 
    FROM retailer.minventory WHERE locn IS NULL;
    end_ts := clock_timestamp();
    RAISE NOTICE '[Init] TTable with missing values (locn): %' , 1000 * (extract(epoch FROM end_ts - start_ts));


    start_ts := clock_timestamp();
    DROP TABLE IF EXISTS tmp_minventory_dateid;
    CREATE TEMPORARY TABLE tmp_minventory_dateid AS
    SELECT locn, ksn, inventoryunits, id 
    FROM retailer.minventory WHERE dateid IS NULL;
    end_ts := clock_timestamp();
    RAISE NOTICE '[Init] TTable with missing values (dateid): %' , 1000 * (extract(epoch FROM end_ts - start_ts));

    -- create tables with imputed values

    start_ts := clock_timestamp();
    DROP TABLE IF EXISTS tmp_imputed_locn;
    CREATE TEMPORARY TABLE tmp_imputed_locn AS
    SELECT id, avg_locn AS locn FROM tmp_minventory_locn;
    end_ts := clock_timestamp();
    RAISE NOTICE '[Init] TTable with imputed values (locn): %' , 1000 * (extract(epoch FROM end_ts - start_ts));

    start_ts := clock_timestamp();
    DROP TABLE IF EXISTS tmp_imputed_dateid;
    CREATE TEMPORARY TABLE tmp_imputed_dateid AS
    SELECT id, avg_dateid AS dateid FROM tmp_minventory_dateid; 
    end_ts := clock_timestamp();
    RAISE NOTICE '[Init] TTable with imputed values (dateid): %' , 1000 * (extract(epoch FROM end_ts - start_ts));

    end_ts2 := end_ts;
    RAISE NOTICE E'[Init] ****** Total time: %\n' , 1000 * (extract(epoch FROM end_ts2 - start_ts2));


    -- superiteration start
    FOR i in 1..4 LOOP

        -- column locn 

        -- cofactor on X_locn- 
        start_ts := clock_timestamp();
        start_ts2 := start_ts;
        SELECT SUM(cont_to_cofactor(tmp_imputed_locn.locn)*
        			cont_to_cofactor(coalesce(tmp_minventory_locn.dateid, tmp_imputed_dateid.dateid))*
                    cont_to_cofactor(tmp_minventory_locn.ksn)*
                    cont_to_cofactor(tmp_minventory_locn.inventoryunits)
               ) 
               INTO STRICT cofactor_locn 
          FROM (tmp_minventory_locn JOIN tmp_imputed_locn 
            ON tmp_minventory_locn.id = tmp_imputed_locn.id) LEFT OUTER JOIN tmp_imputed_dateid 
            ON tmp_minventory_locn.id = tmp_imputed_dateid.id;
        end_ts := clock_timestamp();
        RAISE NOTICE '[locn, %] Cofactor matrix for locn-: %', i, 1000 * (extract(epoch FROM end_ts - start_ts));

        -- train on locn
        start_ts := clock_timestamp();
        params := linear_regression_model(cofactor - cofactor_locn, 0, 0.001, 0, 10000);
        end_ts := clock_timestamp();
        RAISE NOTICE '[locn, %] Training model: %', i, 1000 * (extract(epoch FROM end_ts - start_ts));

        start_ts := clock_timestamp();
        DROP TABLE IF EXISTS tmp_imputed_locn;
        CREATE TEMPORARY TABLE tmp_imputed_locn AS
        SELECT tmp_minventory_locn.id, (
                params[1] + 
                params[2] * coalesce(tmp_minventory_locn.dateid, tmp_imputed_dateid.dateid) +
                params[3] * tmp_minventory_locn.ksn +
                params[4] * tmp_minventory_locn.inventoryunits) AS locn
          FROM (tmp_minventory_locn LEFT OUTER JOIN tmp_imputed_dateid
            ON tmp_minventory_locn.id = tmp_imputed_dateid.id);
        end_ts := clock_timestamp();
        RAISE NOTICE '[locn, %] Computing values into ttable: %', i, 1000 * (extract(epoch FROM end_ts - start_ts));

        start_ts := clock_timestamp();
        SELECT SUM(cont_to_cofactor(tmp_imputed_locn.locn)*
                    cont_to_cofactor(coalesce(tmp_minventory_locn.dateid, tmp_imputed_dateid.dateid))*
                    cont_to_cofactor(tmp_minventory_locn.ksn)*
                    cont_to_cofactor(tmp_minventory_locn.inventoryunits)
               )
               INTO STRICT cofactor_locn_new
          FROM (tmp_minventory_locn JOIN tmp_imputed_locn 
            ON tmp_minventory_locn.id = tmp_imputed_locn.id) LEFT OUTER JOIN tmp_imputed_dateid 
            ON tmp_minventory_locn.id = tmp_imputed_dateid.id;
        end_ts := clock_timestamp();
        RAISE NOTICE '[locn, %] Computing new matrix for locn-: %', i, 1000 * (extract(epoch FROM end_ts - start_ts));


        cofactor := cofactor - cofactor_locn + cofactor_locn_new; 

        end_ts2 := clock_timestamp();
        RAISE NOTICE E'[locn, %] ****** Total time: %\n', i, 1000 * (extract(epoch FROM end_ts2 - start_ts2));

        -- column dateid 
        start_ts := clock_timestamp();
        start_ts2 := start_ts;
        SELECT SUM(cont_to_cofactor(coalesce(tmp_minventory_dateid.locn, tmp_imputed_locn.locn))*
                    cont_to_cofactor(tmp_imputed_dateid.dateid)*
                    cont_to_cofactor(tmp_minventory_dateid.ksn)*
                    cont_to_cofactor(tmp_minventory_dateid.inventoryunits)
                )) 
               INTO STRICT cofactor_dateid 
          FROM (tmp_minventory_dateid JOIN tmp_imputed_dateid 
            ON tmp_minventory_dateid.id = tmp_imputed_dateid.id) LEFT OUTER JOIN tmp_imputed_locn 
            ON tmp_minventory_dateid.id = tmp_imputed_locn.id;
        end_ts := clock_timestamp();
        RAISE NOTICE '[dateid, %] Cofactor matrix for dateid-: %', i, 1000 * (extract(epoch FROM end_ts - start_ts));

        -- train on dateid
        start_ts := clock_timestamp();
        params := linear_regression_model(cofactor - cofactor_dateid, 1, 0.001, 0, 10000);
        end_ts := clock_timestamp();
        RAISE NOTICE '[dateid, %] Training model: %', i, 1000 * (extract(epoch FROM end_ts - start_ts));

        start_ts := clock_timestamp();
        DROP TABLE IF EXISTS tmp_imputed_dateid;
        CREATE TEMPORARY TABLE tmp_imputed_dateid AS
        SELECT tmp_minventory_dateid.id, (
                params[1] + 
                params[2] * coalesce(tmp_minventory_dateid.locn, tmp_imputed_locn.locn) +
                params[3] * tmp_minventory_dateid.ksn +
                params[4] * tmp_minventory_dateid.inventoryunits) AS dateid
          FROM (tmp_minventory_dateid LEFT OUTER JOIN tmp_imputed_locn
            ON tmp_minventory_dateid.id = tmp_imputed_locn.id);
          
        end_ts := clock_timestamp();
        RAISE NOTICE '[dateid, %] Computing values into ttable: %', i, 1000 * (extract(epoch FROM end_ts - start_ts));

        start_ts := clock_timestamp();
        SELECT SUM(cont_to_cofactor(coalesce(tmp_minventory_dateid.locn, tmp_imputed_locn.locn))*
                    cont_to_cofactor(tmp_imputed_dateid.dateid)*
                    cont_to_cofactor(tmp_minventory_dateid.ksn)*
                    cont_to_cofactor(tmp_minventory_dateid.inventoryunits)
               )) 
               INTO STRICT cofactor_dateid_new
          FROM (tmp_minventory_dateid JOIN tmp_imputed_dateid 
            ON tmp_minventory_dateid.id = tmp_imputed_dateid.id) LEFT OUTER JOIN tmp_imputed_locn
            ON tmp_minventory_dateid.id = tmp_imputed_locn.id;
        end_ts := clock_timestamp();
        RAISE NOTICE '[dateid, %] Computing new matrix for dateid-: %', i, 1000 * (extract(epoch FROM end_ts - start_ts));

        cofactor := cofactor - cofactor_dateid + cofactor_dateid_new; 

        end_ts2 := clock_timestamp();
        RAISE NOTICE E'[dateid, %] ****** Total time: %\n', i, 1000 * (extract(epoch FROM end_ts2 - start_ts2));

    END loop;
END$$;
