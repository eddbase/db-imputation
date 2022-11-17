from sqlalchemy import create_engine
import sqlalchemy
from sqlalchemy.orm import sessionmaker

user = 'mindsdb'
password = ''
host = '127.0.0.1'
port = 47335
database = ''

def get_connection():
        return create_engine(url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(user, password, host, port, database))

engine = get_connection()
conn = engine.connect()
print(f"Connection to the {host} for user {user} created successfully.")

engine2 = create_engine("postgresql://massimo:@localhost:5432/postgres")
conn2 = engine2.connect()

Session = sessionmaker(bind=engine2, autocommit=True)
session = Session()


'''
qry = "DROP PREDICTOR mindsdb.crs_dep_hour;"
with engine.begin() as conn:
	resultset = conn.execute(qry)
'''

print("done")
#connection established

def create_join_tables(session, engine2):

    qry ="CREATE OR REPLACE FUNCTION meanimputation() RETURNS int AS $$ DECLARE "\
            "num_columns_null text[] := ARRAY['CRS_DEP_HOUR', 'DISTANCE', 'TAXI_OUT', 'TAXI_IN', 'ARR_DELAY', 'DEP_DELAY']; "\
            "cat_columns_null text[] := ARRAY['DIVERTED']; "\
            "columns_null text[] := num_columns_null || cat_columns_null; "\
            "columns text[] := ARRAY['id', 'airports', 'DISTANCE', 'flight', 'CRS_DEP_HOUR', 'CRS_DEP_MIN', 'CRS_ARR_HOUR', 'CRS_ARR_MIN']; "\
            "_imputed_data float8[]; "\
            "_counter int; "\
            "_query text := 'SELECT '; "\
            "col text;"\
            "_query_2 text := '';"\
            "columns_no_null text[]; "\
            "params text[]; "\
        "BEGIN "\
            "EXECUTE 'DROP TABLE IF EXISTS join_result'; "\
            "EXECUTE 'DROP TABLE IF EXISTS train_table'; "\
            "FOREACH col IN ARRAY columns_null "\
            "LOOP "\
                "_query_2 := _query_2 || format('(CASE WHEN %s IS NOT NULL THEN false ELSE true END) AS %s_is_null, ', col, col); "\
            "end LOOP; "\
            "EXECUTE 'CREATE TABLE tmp_table AS (SELECT * FROM (flight.schedule JOIN flight.distances USING(airports)) AS s JOIN flight.flights USING (flight))'; "\
            "_imputed_data := standard_imputation(num_columns_null, cat_columns_null); "\
            "columns_no_null := array_diff(columns,  num_columns_null || cat_columns_null); "\
            "FOR _counter in 1..(cardinality(columns_no_null)) "\
            "LOOP "\
                "_query := _query || '%s, '; "\
                "params := params || columns_no_null[_counter]; "\
            "end loop; "\
            "FOR _counter in 1..(cardinality(columns_null)) "\
            "LOOP "\
                "_query := _query || '  COALESCE (%s, %s) AS %s, '; "\
                "IF _counter <= cardinality(num_columns_null) then "\
                    "params := params || num_columns_null[_counter] || _imputed_data[_counter]::text || num_columns_null[_counter]; "\
                "ELSE "\
                    "params := params || cat_columns_null[_counter - cardinality(num_columns_null)] || _imputed_data[_counter]::text || cat_columns_null[_counter - cardinality(num_columns_null)]; "\
                "end if; "\
            "end loop; "\
        "EXECUTE ('CREATE TABLE join_result AS (' || format(RTRIM(_query || _query_2, ', '), VARIADIC params) || ' FROM tmp_table)'); "\
        "EXECUTE ('DROP TABLE tmp_table');"\
        "return 0; "\
        "END; "\
    "$$ LANGUAGE plpgsql;"
    session.begin()
    engine2.execute(sqlalchemy.text(qry))
    session.commit()

    from sqlalchemy import func
    session.begin()
    print(session.execute(func.meanimputation()).all())
    session.commit()

create_join_tables(session, engine2)

try:
	with engine.begin() as conn:
	    qry = 'CREATE DATABASE example_db WITH ENGINE = "postgres", PARAMETERS = {"user": "massimo","password": "","host": "host.docker.internal","port": "5432","database": "postgres"};'
	    resultset = conn.execute(qry)
except:
  print("Connection already established") 


mice_iter = 7

col_nulls = ['crs_dep_hour', 'distance', 'taxi_out', 'taxi_in', 'arr_delay', 'dep_delay', 'diverted'];

import time

for i in range(mice_iter):
    for j in range(len(col_nulls)):
        print(col_nulls[j])
        qry = "CREATE PREDICTOR mindsdb."+col_nulls[j]+" FROM example_db (SELECT * FROM join_result WHERE join_result."+col_nulls[j]+"_is_null is false) PREDICT "+col_nulls[j]+\
        " USING model.args={\"submodels\":[{\"module\": \"LightGBM\", \"args\": { \"stop_after\": 12, \"fit_on_dev\": true}}]};"
        with engine.begin() as conn:
            resultset = conn.execute(qry)
        #poll until model is trained
        
        qry = "SELECT status FROM mindsdb.predictors WHERE name = '"+col_nulls[j]+"'"
        with engine.begin() as conn:
            resultset = conn.execute(qry)
            results_as_dict = resultset.mappings().all()
            print(results_as_dict)
            while (results_as_dict[0]['status'] != 'complete'):
                time.sleep(3)
                resultset = conn.execute(qry)
                results_as_dict = resultset.mappings().all()
                print(results_as_dict)
                
        qry = "UPDATE example_db.join_result SET "+col_nulls[j]+" = source.prediction FROM ( "\
          "SELECT t.id, p."+col_nulls[j]+" as prediction "\
          "FROM example_db.join_result as t "\
          "JOIN mindsdb."+col_nulls[j]+" as p WHERE t."+col_nulls[j]+"_is_null is true ) AS source WHERE id = source.id " 
        with engine.begin() as conn:
            print(qry)
            resultset = conn.execute(qry)
            
        qry = "DROP PREDICTOR mindsdb."+col_nulls[j]+";"
        with engine.begin() as conn:
            resultset = conn.execute(qry)



with engine.begin() as conn:
    print(qry)
    resultset = conn.execute(qry)

            




