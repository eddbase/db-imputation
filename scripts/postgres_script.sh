#! /bin/bash

MISSINGNESS=("0.05" "0.1" "0.2" "0.4" "0.6" "0.8")
DATASET="flights"

for value in "${MISSINGNESS[@]}"
do

psql -h /tmp postgres -c 'DROP TABLE join_table'
psql -h /tmp postgres -f '/disk/scratch/imputation_project/postgres/import_${DATASET}_denorm.sql'
psql -h /tmp postgres -c '\copy join_table FROM "data/join_table_${DATASET}_${value}.csv" WITH (FORMAT CSV, NULL '');'

psql -h /tmp postgres -f '/disk/scratch/imputation_project/postgres/MICE_reverse_partitioned_${DATASET}.sql' > postgres_$value_$DATASET_partitioned.txt
psql -h /tmp postgres -f '/disk/scratch/imputation_project/postgres/MICE_partitioned_${DATASET}.sql' > postgres_$value_$DATASET_shared.txt
psql -h /tmp postgres -f '/disk/scratch/imputation_project/postgres/MICE_baseline_${DATASET}.sql' > postgres_$value_$DATASET_baseline.txt

done

