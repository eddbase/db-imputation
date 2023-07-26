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

\copy join_table FROM '/disk/scratch/imputation_project/quality_exp/air_quality_postgres.csv' WITH (FORMAT CSV, NULL '');
CALL MICE_incremental_partitioned('join_table', 'join_table_complete', ARRAY['AQI', 'CO', 'O3', 'PM10', 'PM2_5', 'NO2', 'NOx', 'NO_', 'WindSpeed', 'WindDirec', 'SO2_AVG'], ARRAY[]::text[], ARRAY['CO', 'O3', 'PM10', 'PM2_5', 'NO2', 'NOx', 'NO_', 'WindSpeed', 'WindDirec', 'SO2_AVG'], ARRAY[]::text[]);

CALL MICE_baseline('join_table', 'join_table_complete', ARRAY['AQI', 'CO', 'O3', 'PM10', 'PM2_5', 'NO2', 'NOx', 'NO_', 'WindSpeed', 'WindDirec', 'SO2_AVG'], ARRAY[]::text[], ARRAY['CO', 'O3', 'PM10', 'PM2_5', 'NO2', 'NOx', 'NO_', 'WindSpeed', 'WindDirec', 'SO2_AVG'], ARRAY[]::text[]);

CALL MICE_madlib('join_table', 'join_table_complete', ARRAY['AQI', 'CO', 'O3', 'PM10', 'PM2_5', 'NO2', 'NOx', 'NO_', 'WindSpeed', 'WindDirec', 'SO2_AVG'], ARRAY[]::text[], ARRAY['CO', 'O3', 'PM10', 'PM2_5', 'NO2', 'NOx', 'NO_', 'WindSpeed', 'WindDirec', 'SO2_AVG'], ARRAY[]::text[]);
