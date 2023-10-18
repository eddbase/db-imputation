#! /bin/bash

MISSINGNESS=("0.05" "0.1" "0.2" "0.4" "0.6" "0.8")
DATASET="flights"

for value in "${MISSINGNESS[@]}"
do

psql -h /tmp postgres -c 'DROP TABLE join_table'
psql -h /tmp postgres -f '/disk/scratch/imputation_project/postgres/import_${DATASET}_denorm.sql'
psql -h /tmp postgres -c '\copy join_table FROM "data/join_table_${DATASET}_${value}.csv" WITH (FORMAT CSV, NULL '');'

psql -h /tmp postgres -f '/disk/scratch/imputation_project/postgres/madlib.sql'

if [ "$DATASET" = "flights" ]; then

psql -h /tmp postgres -c "CALL MICE_madlib('join_table', 'join_table_complete', ARRAY['CRS_DEP_HOUR', 'CRS_DEP_MIN', 'CRS_ARR_HOUR', 'CRS_ARR_MIN', 'DISTANCE', 'DEP_DELAY', 'TAXI_OUT', 'TAXI_IN', 'ARR_DELAY', 'ACTUAL_ELAPSED_TIME', 'AIR_TIME', 'DEP_TIME_HOUR', 'DEP_TIME_MIN', 'WHEELS_OFF_HOUR', 'WHEELS_OFF_MIN', 'WHEELS_ON_HOUR', 'WHEELS_ON_MIN', 'ARR_TIME_HOUR', 'ARR_TIME_MIN', 'MONTH_SIN', 'MONTH_COS', 'DAY_SIN', 'DAY_COS', 'WEEKDAY_SIN', 'WEEKDAY_COS'], ARRAY['OP_CARRIER', 'DIVERTED', 'EXTRA_DAY_DEP', 'EXTRA_DAY_ARR']::text[], ARRAY['WHEELS_ON_HOUR', 'WHEELS_OFF_HOUR', 'TAXI_OUT', 'TAXI_IN', 'ARR_DELAY', 'DEP_DELAY'], ARRAY['DIVERTED']::text[]);" > postgres_$value_$DATASET_madlib.txt

else

psql -h /tmp postgres -c "CALL MICE_madlib('join_table', 'join_table_complete', ARRAY['population', 'white', 'asian', 'pacific', 'black', 'medianage', 'occupiedhouseunits', 'houseunits', 'families', 'households', 'husbwife', 'males', 'females', 'householdschildren', 'hispanic', 'rgn_cd', 'clim_zn_nbr', 'tot_area_sq_ft', 'sell_area_sq_ft', 'avghhi', 'supertargetdistance', 'supertargetdrivetime', 'targetdistance', 'targetdrivetime', 'walmartdistance', 'walmartdrivetime', 'walmartsupercenterdistance', 'walmartsupercenterdrivetime' , 'prize', 'feat_1', 'maxtemp', 'mintemp', 'meanwind', 'inventoryunits'], ARRAY['subcategory', 'category', 'categoryCluster', 'rain', 'snow', 'thunder']::text[], ARRAY['inventoryunits', 'maxtemp', 'mintemp', 'supertargetdistance', 'walmartdistance'], ARRAY['rain', 'snow']::text[]);" > postgres_$value_$DATASET_madlib.txt

fi;

done

