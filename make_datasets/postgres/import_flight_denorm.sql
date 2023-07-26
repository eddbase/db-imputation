DROP TABLE IF EXISTS join_table;

CREATE TABLE join_table(
    flight INTEGER,
    airports INTEGER,
    OP_CARRIER INTEGER,
    CRS_DEP_HOUR FLOAT,
    CRS_DEP_MIN FLOAT,
    CRS_ARR_HOUR FLOAT,
    CRS_ARR_MIN FLOAT,
    ORIGIN INTEGER,
    DEST INTEGER,
    DISTANCE FLOAT,
    index INTEGER,
    DEP_DELAY FLOAT,
    TAXI_OUT FLOAT,
    TAXI_IN FLOAT,
    ARR_DELAY FLOAT,
    DIVERTED INTEGER,
    ACTUAL_ELAPSED_TIME FLOAT,
    AIR_TIME FLOAT,
    DEP_TIME_HOUR FLOAT,
    DEP_TIME_MIN FLOAT,
    WHEELS_OFF_HOUR FLOAT,
    WHEELS_OFF_MIN FLOAT,
    WHEELS_ON_HOUR FLOAT,
    WHEELS_ON_MIN FLOAT,
    ARR_TIME_HOUR FLOAT,            -- category(31)
    ARR_TIME_MIN FLOAT,               -- category(10)
    MONTH_SIN FLOAT,        -- category(8)
    MONTH_COS FLOAT,
    DAY_SIN FLOAT,
    DAY_COS FLOAT,
    WEEKDAY_SIN FLOAT,
    WEEKDAY_COS FLOAT,
    EXTRA_DAY_ARR INTEGER,
    EXTRA_DAY_DEP INTEGER
);



\copy join_table FROM 'join_table_flights.csv' WITH (FORMAT CSV, NULL '');