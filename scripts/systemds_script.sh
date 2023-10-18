start_time="$(date -u +%s)"


MISSINGNESS=("0.05" "0.1" "0.2" "0.4" "0.6" "0.8")
DATASET="flights"

for value in "${MISSINGNESS[@]}"
do
echo "bin/systemds mice_${DATASET}.dml -nvargs X=data/join_table_${DATASET}_${value}.csv TYPES=data/cat_numerical.csv"
bin/systemds mice_${DATASET}.dml -nvargs X=data/join_table_${DATASET}_${value}.csv TYPES=data/cat_numerical.csv > systemds_log.txt
done

end_time="$(date -u +%s)"

elapsed="$(($end_time-$start_time))"
echo "Total of $elapsed seconds elapsed for process"
