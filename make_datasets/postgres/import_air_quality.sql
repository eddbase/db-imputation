DROP TABLE IF EXISTS join_table;

CREATE TABLE join_table(
    AQI INTEGER,
    SO2 INTEGER,
    CO INTEGER,
    O3 FLOAT,
    PM10 FLOAT,
    PM2_5 FLOAT,
    NO2 FLOAT,
    NOx INTEGER,
    NO_ INTEGER,
    WindSpeed FLOAT,
    WindDirec INTEGER,
    CO_8hr FLOAT,
    SO2_AVG FLOAT
);

\copy join_table FROM 'air_quality.csv' WITH (FORMAT CSV, NULL '');