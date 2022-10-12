SET work_mem='1GB';

DO $$
DECLARE
    start_ts timestamptz;
    end_ts   timestamptz;
    cnt      int;
BEGIN
    RAISE NOTICE 'SUM(1) benchmarks for the flights dataset';
    RAISE NOTICE '--------------------------------------------------';

    -- SUM(1) FROM Flight
    start_ts := clock_timestamp();
    SELECT SUM(1) INTO STRICT cnt FROM flights.Flight;
    end_ts := clock_timestamp();
    RAISE NOTICE 'SUM(1) FROM Flight. cnt = %, ms = %', cnt, 1000 * (extract(epoch FROM end_ts - start_ts));

    -- SUM(1) FROM full join
    start_ts := clock_timestamp();
    SELECT SUM(1) INTO STRICT cnt
    FROM flights.Flight AS F
    JOIN flights.Schedule AS S ON F.SCHEDULE_ID = S.SCHEDULE_ID
    JOIN flights.Route AS R ON S.ROUTE_ID = R.ROUTE_ID;
    end_ts := clock_timestamp();
    RAISE NOTICE 'SUM(1) FROM full join. cnt = %, ms = %', cnt, 1000 * (extract(epoch FROM end_ts - start_ts));

    -- SUM(1) FROM factorized join: F * (S * R)
    start_ts := clock_timestamp();
    SELECT SUM(t.cnt) INTO STRICT cnt
    FROM flights.Flight AS F 
    JOIN (
        SELECT S.SCHEDULE_ID, SUM(1) AS cnt
          FROM flights.Schedule AS S
          JOIN flights.Route AS R
            ON S.ROUTE_ID = R.ROUTE_ID
      GROUP BY S.SCHEDULE_ID
    ) AS t
      ON F.SCHEDULE_ID = t.SCHEDULE_ID;
    end_ts := clock_timestamp();
    RAISE NOTICE 'SUM(1) FROM factorized join: F * (S * R). cnt = %, ms = %', cnt, 1000 * (extract(epoch FROM end_ts - start_ts));

    -- SUM(1) FROM factorized join: (F * S) * R
    start_ts := clock_timestamp();
    SELECT SUM(t.cnt) INTO STRICT cnt
    FROM (
        SELECT S.ROUTE_ID, SUM(1) AS cnt
          FROM flights.Flight AS F
          JOIN flights.Schedule AS S
            ON F.SCHEDULE_ID = S.SCHEDULE_ID
      GROUP BY S.ROUTE_ID
    ) AS t
    JOIN flights.Route AS R
      ON t.ROUTE_ID = R.ROUTE_ID;
    end_ts := clock_timestamp();
    RAISE NOTICE 'SUM(1) FROM factorized join: (F * S) * R. cnt = %, ms = %', cnt, 1000 * (extract(epoch FROM end_ts - start_ts));

    -- SUM(1) FROM factorized join: F * (S * R) + single-table aggregations (excluding primary keys)
    start_ts := clock_timestamp();
    SELECT SUM(F.cnt * t.cnt) INTO STRICT cnt
    FROM (
        SELECT F.SCHEDULE_ID, SUM(1) AS cnt
          FROM flights.Flight AS F
      GROUP BY F.SCHEDULE_ID
    ) AS F
    JOIN (
        SELECT S.SCHEDULE_ID, SUM(1) AS cnt
          FROM flights.Schedule AS S
          JOIN flights.Route AS R
            ON S.ROUTE_ID = R.ROUTE_ID
      GROUP BY S.SCHEDULE_ID
    ) AS t
      ON F.SCHEDULE_ID = t.SCHEDULE_ID;
    end_ts := clock_timestamp();
    end_ts := clock_timestamp();
    RAISE NOTICE 'SUM(1) FROM factorized join: F * (S * R) + single-table aggregations (excluding primary keys). cnt = %, ms = %', cnt, 1000 * (extract(epoch FROM end_ts - start_ts));
END$$;

DO $$
DECLARE
    start_ts timestamptz;
    end_ts   timestamptz;
    cof      cofactor;
BEGIN
    RAISE NOTICE 'SUM(cofactor continuous only) benchmarks for the flights dataset';
    RAISE NOTICE '----------------------------------------------------------------';

    -- SUM(cofactor continuous only) FROM full join
    start_ts := clock_timestamp();
    SELECT SUM(to_cofactor(
        ARRAY[
            R.DISTANCE,
            S.CRS_DEP_HOUR, S.CRS_DEP_MIN, S.CRS_ARR_HOUR, S.CRS_ARR_MIN, S.CRS_ELAPSED_TIME,
            F.DEP_DELAY, F.TAXI_OUT, F.TAXI_IN, F.ARR_DELAY, F.ACTUAL_ELAPSED_TIME,
            F.AIR_TIME, F.DEP_TIME_HOUR, F.DEP_TIME_MIN, F.WHEELS_OFF_HOUR, F.WHEELS_OFF_MIN,
            F.WHEELS_ON_HOUR, F.WHEELS_ON_MIN, F.ARR_TIME_HOUR, F.ARR_TIME_MIN
        ],
        ARRAY[
            -- R.ORIGIN, R.DEST, S.OP_CARRIER, F.DIVERTED
            -- R.ORIGIN, R.DEST, S.OP_CARRIER, S.OP_CARRIER_FL_NUM, F.DIVERTED
        ]::int4[]
    )) INTO STRICT cof
    FROM flights.Flight AS F
    JOIN flights.Schedule AS S ON F.SCHEDULE_ID = S.SCHEDULE_ID
    JOIN flights.Route AS R ON S.ROUTE_ID = R.ROUTE_ID;
    end_ts := clock_timestamp();
    RAISE NOTICE 'SUM(cofactor continuous only) FROM full join. ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
    -- RAISE NOTICE 'SUM(cofactor continuous only) FROM full join. cof = %, ms = %', cof, 1000 * (extract(epoch FROM end_ts - start_ts));

    CALL cofactor_stats(cof);

--     FOR i IN 1..5 LOOP

    -- SUM(cofactor continuous only) FROM factorized join: F * (S * R)
    start_ts := clock_timestamp();
    SELECT SUM(to_cofactor(
        ARRAY[
            F.DEP_DELAY, F.TAXI_OUT, F.TAXI_IN, F.ARR_DELAY, F.ACTUAL_ELAPSED_TIME,
            F.AIR_TIME, F.DEP_TIME_HOUR, F.DEP_TIME_MIN, F.WHEELS_OFF_HOUR, F.WHEELS_OFF_MIN,
            F.WHEELS_ON_HOUR, F.WHEELS_ON_MIN, F.ARR_TIME_HOUR, F.ARR_TIME_MIN
        ],
        ARRAY[
            -- F.DIVERTED
        ]::int4[]
    ) * t.cnt) INTO STRICT cof
    FROM flights.Flight AS F
    JOIN (
        SELECT S.SCHEDULE_ID, SUM(to_cofactor(
            ARRAY[
                R.DISTANCE,
                S.CRS_DEP_HOUR, S.CRS_DEP_MIN, S.CRS_ARR_HOUR, S.CRS_ARR_MIN, S.CRS_ELAPSED_TIME
            ],
            ARRAY[
                -- R.ORIGIN, R.DEST, S.OP_CARRIER
            ]::int4[]
        )) AS cnt
          FROM flights.Schedule AS S
          JOIN flights.Route AS R
            ON S.ROUTE_ID = R.ROUTE_ID
      GROUP BY S.SCHEDULE_ID
    ) AS t
      ON F.SCHEDULE_ID = t.SCHEDULE_ID;
    end_ts := clock_timestamp();
    RAISE NOTICE 'SUM(cofactor continuous only) FROM factorized join: F * (S * R). ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
    -- RAISE NOTICE 'SUM(cofactor continuous only) FROM factorized join: F * (S * R). cof = %, ms = %', cof, 1000 * (extract(epoch FROM end_ts - start_ts));
    
    CALL cofactor_stats(cof);

    -- SUM(cofactor continuous only) FROM factorized join: (F * S) * R
    start_ts := clock_timestamp();
    SELECT SUM(to_cofactor(
        ARRAY[
            R.DISTANCE
        ],
        ARRAY[
            -- R.ORIGIN, R.DEST
        ]::int4[]
    ) * t.cnt) INTO STRICT cof
    FROM (
        SELECT S.ROUTE_ID, SUM(to_cofactor(
            ARRAY[
                S.CRS_DEP_HOUR, S.CRS_DEP_MIN, S.CRS_ARR_HOUR, S.CRS_ARR_MIN, S.CRS_ELAPSED_TIME,
                F.DEP_DELAY, F.TAXI_OUT, F.TAXI_IN, F.ARR_DELAY, F.ACTUAL_ELAPSED_TIME,
                F.AIR_TIME, F.DEP_TIME_HOUR, F.DEP_TIME_MIN, F.WHEELS_OFF_HOUR, F.WHEELS_OFF_MIN,
                F.WHEELS_ON_HOUR, F.WHEELS_ON_MIN, F.ARR_TIME_HOUR, F.ARR_TIME_MIN                
            ],
            ARRAY[
                -- S.OP_CARRIER, F.DIVERTED
            ]::int4[]
        )) AS cnt
          FROM flights.Flight AS F
          JOIN flights.Schedule AS S
            ON F.SCHEDULE_ID = S.SCHEDULE_ID
      GROUP BY S.ROUTE_ID
    ) AS t
    JOIN flights.Route AS R
      ON t.ROUTE_ID = R.ROUTE_ID;
    end_ts := clock_timestamp();
    RAISE NOTICE 'SUM(cofactor continuous only) FROM factorized join: (F * S) * R. ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
    -- RAISE NOTICE 'SUM(cofactor continuous only) FROM factorized join: (F * S) * R. cof = %, ms = %', cof, 1000 * (extract(epoch FROM end_ts - start_ts));

    CALL cofactor_stats(cof);

--     END LOOP;
END$$;

DO $$
DECLARE
    start_ts timestamptz;
    end_ts   timestamptz;
    cof      cofactor;
BEGIN
    RAISE NOTICE 'SUM(cofactor) benchmarks for the flights dataset';
    RAISE NOTICE '--------------------------------------------------';

    -- SUM(cofactor) FROM full join
    start_ts := clock_timestamp();
    SELECT SUM(to_cofactor(
        ARRAY[
            R.DISTANCE,
            S.CRS_DEP_HOUR, S.CRS_DEP_MIN, S.CRS_ARR_HOUR, S.CRS_ARR_MIN, S.CRS_ELAPSED_TIME,
            F.DEP_DELAY, F.TAXI_OUT, F.TAXI_IN, F.ARR_DELAY, F.ACTUAL_ELAPSED_TIME,
            F.AIR_TIME, F.DEP_TIME_HOUR, F.DEP_TIME_MIN, F.WHEELS_OFF_HOUR, F.WHEELS_OFF_MIN,
            F.WHEELS_ON_HOUR, F.WHEELS_ON_MIN, F.ARR_TIME_HOUR, F.ARR_TIME_MIN
        ],
        ARRAY[
            R.ORIGIN, R.DEST, S.OP_CARRIER, F.DIVERTED
            -- R.ORIGIN, R.DEST, S.OP_CARRIER, S.OP_CARRIER_FL_NUM, F.DIVERTED
        ]::int4[]
    )) INTO STRICT cof
    FROM flights.Flight AS F
    JOIN flights.Schedule AS S ON F.SCHEDULE_ID = S.SCHEDULE_ID
    JOIN flights.Route AS R ON S.ROUTE_ID = R.ROUTE_ID;
    end_ts := clock_timestamp();
    RAISE NOTICE 'SUM(cofactor) FROM full join. ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
    -- RAISE NOTICE 'SUM(cofactor) FROM full join. cof = %, ms = %', cof, 1000 * (extract(epoch FROM end_ts - start_ts));

    CALL cofactor_stats(cof);

--     FOR i IN 1..5 LOOP

    -- SUM(cofactor) FROM factorized join: F * (S * R)
    start_ts := clock_timestamp();
    SELECT SUM(to_cofactor(
        ARRAY[
            F.DEP_DELAY, F.TAXI_OUT, F.TAXI_IN, F.ARR_DELAY, F.ACTUAL_ELAPSED_TIME,
            F.AIR_TIME, F.DEP_TIME_HOUR, F.DEP_TIME_MIN, F.WHEELS_OFF_HOUR, F.WHEELS_OFF_MIN,
            F.WHEELS_ON_HOUR, F.WHEELS_ON_MIN, F.ARR_TIME_HOUR, F.ARR_TIME_MIN
        ],
        ARRAY[
            F.DIVERTED
        ]
    ) * t.cnt) INTO STRICT cof
    FROM flights.Flight AS F
    JOIN (
        SELECT S.SCHEDULE_ID, SUM(to_cofactor(
            ARRAY[
                R.DISTANCE,
                S.CRS_DEP_HOUR, S.CRS_DEP_MIN, S.CRS_ARR_HOUR, S.CRS_ARR_MIN, S.CRS_ELAPSED_TIME
            ],
            ARRAY[
                R.ORIGIN, R.DEST, S.OP_CARRIER
            ]
        )) AS cnt
          FROM flights.Schedule AS S
          JOIN flights.Route AS R
            ON S.ROUTE_ID = R.ROUTE_ID
      GROUP BY S.SCHEDULE_ID
    ) AS t
      ON F.SCHEDULE_ID = t.SCHEDULE_ID;
    end_ts := clock_timestamp();
    RAISE NOTICE 'SUM(cofactor) FROM factorized join: F * (S * R). ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
    -- RAISE NOTICE 'SUM(cofactor) FROM factorized join: F * (S * R). cof = %, ms = %', cof, 1000 * (extract(epoch FROM end_ts - start_ts));
    
    CALL cofactor_stats(cof);

    -- SUM(cofactor) FROM factorized join: (F * S) * R
    start_ts := clock_timestamp();
    SELECT SUM(to_cofactor(
        ARRAY[
            R.DISTANCE
        ],
        ARRAY[
            R.ORIGIN, R.DEST
        ]
    ) * t.cnt) INTO STRICT cof
    FROM (
        SELECT S.ROUTE_ID, SUM(to_cofactor(
            ARRAY[
                S.CRS_DEP_HOUR, S.CRS_DEP_MIN, S.CRS_ARR_HOUR, S.CRS_ARR_MIN, S.CRS_ELAPSED_TIME,
                F.DEP_DELAY, F.TAXI_OUT, F.TAXI_IN, F.ARR_DELAY, F.ACTUAL_ELAPSED_TIME,
                F.AIR_TIME, F.DEP_TIME_HOUR, F.DEP_TIME_MIN, F.WHEELS_OFF_HOUR, F.WHEELS_OFF_MIN,
                F.WHEELS_ON_HOUR, F.WHEELS_ON_MIN, F.ARR_TIME_HOUR, F.ARR_TIME_MIN                
            ],
            ARRAY[
                S.OP_CARRIER, F.DIVERTED
            ]
        )) AS cnt
          FROM flights.Flight AS F
          JOIN flights.Schedule AS S
            ON F.SCHEDULE_ID = S.SCHEDULE_ID
      GROUP BY S.ROUTE_ID
    ) AS t
    JOIN flights.Route AS R
      ON t.ROUTE_ID = R.ROUTE_ID;
    end_ts := clock_timestamp();
    RAISE NOTICE 'SUM(cofactor) FROM factorized join: (F * S) * R. ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
    -- RAISE NOTICE 'SUM(cofactor) FROM factorized join: (F * S) * R. cof = %, ms = %', cof, 1000 * (extract(epoch FROM end_ts - start_ts));

    CALL cofactor_stats(cof);

--     END LOOP;
END$$;