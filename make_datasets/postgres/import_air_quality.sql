DROP TABLE IF EXISTS join_table;

CREATE TABLE join_table(
    AQI FLOAT,
    CO FLOAT,
    O3 FLOAT,
    PM10 FLOAT,
    PM2_5 FLOAT,
    NO2 FLOAT,
    NOx FLOAT,
    NO_ FLOAT,
    WindSpeed FLOAT,
    WindDirec FLOAT,
    SO2_AVG FLOAT
);

\copy join_table FROM 'air_quality_postgres.csv' WITH (FORMAT CSV, NULL '');