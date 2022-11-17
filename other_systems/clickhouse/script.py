from sqlalchemy import create_engine
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from clickhouse_driver import Client

client = Client('localhost')


qry = "DROP TABLE IF EXISTS join_result;"
client.execute(qry)

qry = "DROP TABLE IF EXISTS train_table;"
client.execute(qry)

qry = "CREATE temporary TABLE tmp_table_2 AS (SELECT * FROM flight.schedule JOIN flight.distances USING(airports));"
client.execute(qry)

qry = "CREATE temporary TABLE tmp_table AS (SELECT * FROM tmp_table_2 JOIN flight.flights USING (flight));"
client.execute(qry)

qry = "DROP TABLE tmp_table_2;"
client.execute(qry)

client.execute("DROP TABLE IF EXISTS flight.join_result;")
client.execute("DROP TABLE IF EXISTS res;")

num_columns_null =['CRS_DEP_HOUR', 'DISTANCE', 'TAXI_OUT', 'TAXI_IN', 'ARR_DELAY', 'DEP_DELAY']
cat_columns_null =['DIVERTED']

def build_avg_query(num_columns_null, cat_columns_null):
    query = "SELECT "
    for col in num_columns_null:
        query += "AVG(CASE WHEN "+col+" IS NOT NULL THEN "+col+" END), "
        
    for col in cat_columns_null:
        query += "topK(1)(CASE WHEN "+col+" IS NOT NULL THEN "+col+" END), "
        
    query = query.rstrip(', ') + " FROM tmp_table;"
    return query


def build_coalesce_query(num_columns_null, cat_columns_null, columns, averages):
    query = "SELECT "
    query_2 = ""
    col_no_nulls = list((set(columns) - set(num_columns_null)) - set(cat_columns_null))
    query += (', '.join(col_no_nulls)) +", "
    for col in range(len(num_columns_null)):
        query += "COALESCE ("+num_columns_null[col]+", "+str(averages[col])+") AS "+num_columns_null[col]+", "
        query_2 += "(CASE WHEN tmp_table."+num_columns_null[col]+" IS NOT NULL THEN false ELSE true END) AS "+num_columns_null[col]+"_is_null, "
        
    for col in range(len(cat_columns_null)):
        query += "COALESCE ("+cat_columns_null[col]+", "+str(averages[col+len(num_columns_null)][0])+") AS "+cat_columns_null[col]+", "
        query_2 += "(CASE WHEN tmp_table."+cat_columns_null[col]+" IS NOT NULL THEN false ELSE true END) AS "+cat_columns_null[col]+"_is_null, "
        
    query = query.rstrip(', ') +" , "+ query_2.rstrip(', ') + " FROM tmp_table"
    return query

query = build_avg_query(num_columns_null, cat_columns_null)
default_imputation = client.execute(query)

columns = ['id', 'airports', 'DISTANCE', 'flight', 'CRS_DEP_HOUR', 'CRS_DEP_MIN', 'CRS_ARR_HOUR', 'CRS_ARR_MIN', 'DEP_DELAY', 'TAXI_OUT', 'TAXI_IN', 'ARR_DELAY', 'DIVERTED', 'ACTUAL_ELAPSED_TIME', 'AIR_TIME', 'DEP_TIME_HOUR', 'DEP_TIME_MIN', 'WHEELS_OFF_HOUR', 'WHEELS_OFF_MIN', 'WHEELS_ON_HOUR', \
           'WHEELS_ON_MIN', 'ARR_TIME_HOUR', 'ARR_TIME_MIN', 'MONTH_SIN', 'MONTH_COS', 'DAY_SIN', 'DAY_COS', 'WEEKDAY_SIN', 'WEEKDAY_COS', 'EXTRA_DAY_ARR', 'EXTRA_DAY_DEP']

query = 'CREATE TABLE flight.join_result ENGINE = MergeTree() PRIMARY KEY id AS (' + build_coalesce_query(num_columns_null, cat_columns_null, columns, default_imputation[0])+")";
print(query)
res = client.execute(query)

for i in range (7):
    for c in num_columns_null:
        indep_vars = columns.copy()
        indep_vars.remove(c)
        query = "CREATE TABLE "+c+"_model ENGINE = Memory AS SELECT stochasticLinearRegressionState(0.00001, 0, 1, 'Adam')("+c+", "+", ".join(indep_vars)+") AS state FROM flight.join_result WHERE "+c+"_is_null == false;"
        client.execute(query)
        client.execute("CREATE TABLE res (id integer, result float) Engine = Join(ANY, LEFT, id)")
        client.execute("INSERT INTO res WITH (SELECT state FROM "+c+"_model) AS model SELECT id, evalMLMethod(model, "+", ".join(indep_vars)+") FROM flight.join_result WHERE "+c+"_is_null == true")
        client.execute("ALTER TABLE flight.join_result UPDATE "+c+"= joinGet('res', 'result', id) WHERE "+c+"_is_null == true;")
        client.execute("drop table "+c+"_model")
        client.execute("DROP TABLE res;")
        
    for c in cat_columns_null:
        indep_vars = columns.copy()
        indep_vars.remove(c)
        query = "CREATE TABLE "+c+"_model ENGINE = Memory AS SELECT stochasticLogisticRegressionState(0.00001, 0, 1, 'Adam')("+c+", "+", ".join(indep_vars)+") AS state FROM flight.join_result WHERE "+c+"_is_null == false;"
        client.execute(query)
        client.execute("CREATE TABLE res (id integer, result float) Engine = Join(ANY, LEFT, id)")
        client.execute("INSERT INTO res WITH (SELECT state FROM "+c+"_model) AS model SELECT id, evalMLMethod(model, "+", ".join(indep_vars)+") FROM flight.join_result WHERE "+c+"_is_null == true")
        client.execute("ALTER TABLE flight.join_result UPDATE "+c+"= joinGet('res', 'result', id) WHERE "+c+"_is_null == true;")
        client.execute("drop table "+c+"_model")
        client.execute("DROP TABLE res;")
