SET work_mem='1GB';
\pset tuples_only

DO $$
DECLARE
    start_ts timestamptz;
    end_ts   timestamptz;
    cnt      int;
BEGIN
    RAISE NOTICE 'SUM(1) benchmarks for the retailer dataset';
    RAISE NOTICE '--------------------------------------------------';

    -- SUM(1) FROM Inventory
    start_ts := clock_timestamp();
    SELECT SUM(1) INTO STRICT cnt FROM retailer.Inventory;
    end_ts := clock_timestamp();
    RAISE NOTICE 'SUM(1) FROM Inventory. cnt = %, ms = %', cnt, 1000 * (extract(epoch FROM end_ts - start_ts));

    -- SUM(1) FROM full join
    start_ts := clock_timestamp();
    SELECT SUM(1) INTO STRICT cnt
    FROM retailer.Inventory AS Inv 
    JOIN retailer.Weather AS W ON Inv.locn = W.locn AND Inv.dateid = W.dateid
    JOIN retailer.Item AS It ON Inv.ksn = It.ksn
    JOIN retailer.Location AS L ON Inv.locn = L.locn
    JOIN retailer.Census AS C ON L.zip = C.zip;
    end_ts := clock_timestamp();
    RAISE NOTICE 'SUM(1) FROM full join. cnt = %, ms = %', cnt, 1000 * (extract(epoch FROM end_ts - start_ts));

    -- SUM(1) FROM factorized join
    start_ts := clock_timestamp();
    SELECT SUM(t2.cnt * t3.cnt) INTO STRICT cnt
    FROM (-- 6.1s
            SELECT t1.locn, SUM(t1.cnt) AS cnt
            FROM (-- 5.9s   
                    SELECT Inv.locn, Inv.dateid, SUM(1) AS cnt
                    FROM retailer.Inventory AS Inv
                    JOIN retailer.Item AS It ON Inv.ksn = It.ksn
                GROUP BY Inv.locn, Inv.dateid
                ) AS t1   
            JOIN retailer.Weather AS W ON t1.locn = W.locn AND t1.dateid = W.dateid
            GROUP BY t1.locn
        ) AS t2
    JOIN (-- 0.01s
            SELECT L.locn, SUM(1) AS cnt
            FROM retailer.Location AS L 
            JOIN retailer.Census AS C ON L.zip = C.zip
        GROUP BY L.locn
        ) AS t3
    ON t2.locn = t3.locn;
    end_ts := clock_timestamp();
    RAISE NOTICE 'SUM(1) FROM factorized join. cnt = %, ms = %', cnt, 1000 * (extract(epoch FROM end_ts - start_ts));

    -- SUM(1) FROM factorized join + single-table aggregations
    start_ts := clock_timestamp();
    SELECT SUM(t2.cnt * t3.cnt) INTO STRICT cnt
    FROM (-- 33.2s
            SELECT t1.locn, SUM(t1.cnt * t13.cnt) AS cnt
            FROM (-- 31.4s (initial 83.4s)   
                    SELECT t11.locn, t11.dateid, SUM(t11.cnt * t12.cnt) AS cnt
                    FROM (
                            SELECT Inv.locn, Inv.dateid, Inv.ksn, SUM(1) AS cnt
                            FROM retailer.Inventory AS Inv
                            GROUP BY Inv.locn, Inv.dateid, Inv.ksn
                        ) AS t11  
                    JOIN (
                            SELECT It.ksn, SUM(1) AS cnt
                            FROM retailer.Item AS It 
                            GROUP BY It.ksn
                        ) AS t12
                        ON t11.ksn = t12.ksn
                    GROUP BY t11.locn, t11.dateid
                ) AS t1   
            JOIN (
                    SELECT W.locn, W.dateid, SUM(1) AS cnt
                    FROM retailer.Weather AS W 
                    GROUP BY W.locn, W.dateid
                ) AS t13
            ON t1.locn = t13.locn AND t1.dateid = t13.dateid
            GROUP BY t1.locn
        ) AS t2
    JOIN (   -- 0.01s
            SELECT t31.locn, SUM(t31.cnt * t32.cnt) AS cnt
            FROM (  SELECT L.locn, L.zip, SUM(1) AS cnt
                        FROM retailer.Location AS L
                    GROUP BY L.locn, L.zip
                    ) AS t31
            JOIN (  SELECT C.zip, SUM(1) AS cnt 
                        FROM retailer.Census AS C 
                    GROUP BY C.zip
                    ) AS t32
                ON t31.zip = t32.zip
            GROUP BY t31.locn
        ) AS t3
    ON t2.locn = t3.locn;
    end_ts := clock_timestamp();
    RAISE NOTICE 'SUM(1) FROM factorized join + single-table aggregations. cnt = %, ms = %', cnt, 1000 * (extract(epoch FROM end_ts - start_ts));
END$$;

DO $$
DECLARE
    start_ts timestamptz;
    end_ts   timestamptz;
    cof      cofactor;
BEGIN
    RAISE NOTICE 'SUM(cofactor) benchmarks for the retailer dataset';
    RAISE NOTICE '--------------------------------------------------';

    -- -- SUM(cofactor) FROM full join
    -- start_ts := clock_timestamp();
    -- SELECT SUM(to_cofactor(
    --     ARRAY[
    --         C.population, C.white, C.asian, C.pacific, C.black, C.medianage, 
    --         C.occupiedhouseunits, C.houseunits, C.families, C.households, 
    --         C.husbwife, C.males, C.females, C.householdschildren, C.hispanic,
    --         L.rgn_cd, L.clim_zn_nbr, L.tot_area_sq_ft, L.sell_area_sq_ft, L.avghhi, 
    --         L.supertargetdistance, L.supertargetdrivetime, L.targetdistance, 
    --         L.targetdrivetime, L.walmartdistance, L.walmartdrivetime, 
    --         L.walmartsupercenterdistance, L.walmartsupercenterdrivetime,
    --         It.prize,
    --         W.maxtemp, W.mintemp, W.meanwind,
    --         Inv.inventoryunits
    --     ],
    --     ARRAY[
    --         It.subcategory, It.category, It.categoryCluster, 
    --         W.rain, W.snow, W.thunder
    --     ]
    -- )) INTO STRICT cof
    -- FROM retailer.Inventory AS Inv 
    -- JOIN retailer.Weather AS W ON Inv.locn = W.locn AND Inv.dateid = W.dateid
    -- JOIN retailer.Item AS It ON Inv.ksn = It.ksn
    -- JOIN retailer.Location AS L ON Inv.locn = L.locn
    -- JOIN retailer.Census AS C ON L.zip = C.zip;
    -- end_ts := clock_timestamp();
    -- RAISE NOTICE 'SUM(cofactor) FROM full join. cof = %, ms = %', cof, 1000 * (extract(epoch FROM end_ts - start_ts));

    FOR i IN 1..5 LOOP

    -- SUM(cofactor) FROM factorized join
    start_ts := clock_timestamp();
    SELECT SUM(t2.cnt * t3.cnt) INTO STRICT cof
    FROM (-- 28.1s
            SELECT t1.locn, 
                SUM(t1.cnt * to_cofactor(
                        ARRAY[W.maxtemp, W.mintemp, W.meanwind],
                        ARRAY[W.rain, W.snow, W.thunder]
                    )) AS cnt
            FROM (-- 58.4s
                    SELECT Inv.locn, Inv.dateid,
                        SUM(to_cofactor(
                            ARRAY[Inv.inventoryunits, It.prize],
                            ARRAY[It.subcategory, It.category, It.categoryCluster]
                        )) AS cnt                            
                    FROM retailer.Inventory AS Inv
                    JOIN retailer.Item AS It ON Inv.ksn = It.ksn
                GROUP BY Inv.locn, Inv.dateid
                ) AS t1
            JOIN retailer.Weather AS W ON t1.locn = W.locn AND t1.dateid = W.dateid
            GROUP BY t1.locn
        ) AS t2
    JOIN (-- 0.01s
            SELECT L.locn, 
                    SUM(to_cofactor(
                            ARRAY[
                                C.population, C.white, C.asian, C.pacific, C.black, C.medianage, 
                                C.occupiedhouseunits, C.houseunits, C.families, C.households, 
                                C.husbwife, C.males, C.females, C.householdschildren, C.hispanic,
                                L.rgn_cd, L.clim_zn_nbr, L.tot_area_sq_ft, L.sell_area_sq_ft, L.avghhi, 
                                L.supertargetdistance, L.supertargetdrivetime, L.targetdistance, 
                                L.targetdrivetime, L.walmartdistance, L.walmartdrivetime, 
                                L.walmartsupercenterdistance, L.walmartsupercenterdrivetime
                            ],
                            ARRAY[]::int4[]
                        )) AS cnt
            FROM retailer.Location AS L 
            JOIN retailer.Census AS C ON L.zip = C.zip
        GROUP BY L.locn
        ) AS t3
    ON t2.locn = t3.locn;
    end_ts := clock_timestamp();
    RAISE NOTICE 'SUM(cofactor) FROM factorized join. ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
    -- RAISE NOTICE 'SUM(cofactor) FROM factorized join. cof = %, ms = %', cof, 1000 * (extract(epoch FROM end_ts - start_ts));
    
    END LOOP;

    CALL cofactor_stats(cof);    
END$$;