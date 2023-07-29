from sqlalchemy import create_engine
import sqlalchemy
from sqlalchemy.orm import sessionmaker
#python -m mindsdb

user = 'mindsdb'
password = ''
host = '127.0.0.1'
port = 47335
database = ''
user_postgres = 'username'

def get_connection():
        return create_engine(url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(user, password, host, port, database))

engine = get_connection()
conn = engine.connect()
print(f"Connection to the {host} for user {user} created successfully.")

engine2 = create_engine("postgresql://"+user_postgres+":@localhost:5432/postgres")
conn2 = engine2.connect()

#Session = sessionmaker(bind=engine2, autocommit=True)
#session = Session()


'''
qry = "DROP PREDICTOR mindsdb.crs_dep_hour;"
with engine.begin() as conn:
	resultset = conn.execute(qry)
'''

print("done")
#connection established

def create_join_tables(engine2):

    qry ="""
    CREATE OR REPLACE PROCEDURE generate_output_table(
        input_table_name text,
        output_table_name text,
        continuous_columns text[],
        categorical_columns text[],
        continuous_columns_null text[],
        categorical_columns_null text[]
    ) LANGUAGE plpgsql AS $$
DECLARE
    start_ts timestamptz;
    end_ts   timestamptz;
    query text;
    query2 text;
    col_averages float8[];
    tmp_array text[];
    tmp_array2 text[];
    col_mode int4[];
    col text;
    BEGIN
    -- COMPUTE COLUMN AVERAGES (over a subset)
    SELECT array_agg('AVG(' || x || ')')
    FROM unnest(continuous_columns_null) AS x
    INTO tmp_array;
    
    SELECT array_agg('MODE() WITHIN GROUP (ORDER BY ' || x || ') ')
    FROM unnest(categorical_columns_null) AS x
    INTO tmp_array2;

    query := ' SELECT ARRAY[ ' || array_to_string(tmp_array, ', ') || ' ]::float8[]' ||
             ' FROM ( SELECT ' || array_to_string(continuous_columns_null, ', ') ||
                    ' FROM ' || input_table_name || ' LIMIT 500000 ) AS t';
    query2 := ' SELECT ARRAY[ ' || array_to_string(tmp_array2, ', ') || ' ]::int[]' ||
             ' FROM ( SELECT ' || array_to_string(categorical_columns_null, ', ') ||
                    ' FROM ' || input_table_name || ' LIMIT 500000 ) AS t';

    RAISE DEBUG '%', query;

    start_ts := clock_timestamp();
    IF array_length(continuous_columns_null, 1) > 0 THEN
    EXECUTE query INTO col_averages;
    END IF;
    IF array_length(categorical_columns_null, 1) > 0 THEN
        EXECUTE query2 INTO col_mode;
    END IF;
    end_ts := clock_timestamp();
    

    RAISE DEBUG 'AVERAGES: %', col_averages;
    RAISE INFO 'COMPUTE COLUMN AVERAGES: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

    -- CREATE TABLE WITH MISSING VALUES
    query := 'DROP TABLE IF EXISTS ' || output_table_name;
    RAISE DEBUG '%', query;
    EXECUTE QUERY;
    
    
    SELECT (
        SELECT array_agg(x || ' float8')
        FROM unnest(continuous_columns) AS x
    ) ||
    (
        SELECT array_agg(x || ' int')
        FROM unnest(categorical_columns) AS x
    ) ||
    (
        SELECT array_agg(x || '_ISNULL bool')
        FROM unnest(continuous_columns_null) AS x
    )||
    (
        SELECT array_agg(x || '_ISNULL bool')
        FROM unnest(categorical_columns_null) AS x
    )
    INTO tmp_array;
        
    query := 'CREATE TABLE ' || output_table_name || '( ' ||
                array_to_string(tmp_array, ', ') || ', ROW_ID serial) WITH (fillfactor=75)';
    RAISE DEBUG '%', query;
    EXECUTE QUERY;
    
          
        -- INSERT INTO TABLE WITH MISSING VALUES
    start_ts := clock_timestamp();
    SELECT (
        SELECT array_agg(
            CASE
                WHEN array_position(continuous_columns_null, x) IS NULL THEN
                    x
                ELSE
                    'COALESCE(' || x || ', ' || col_averages[array_position(continuous_columns_null, x)] || ')'
            END
        )
        FROM unnest(continuous_columns) AS x
    ) || (
        SELECT array_agg(
            CASE
                WHEN array_position(categorical_columns_null, x) IS NULL THEN
                    x
                ELSE
                    'COALESCE(' || x || ', ' || col_mode[array_position(categorical_columns_null, x)] || ')'
            END
        )
        FROM unnest(categorical_columns) AS x
    ) || (
        SELECT array_agg(x || ' IS NULL')
        FROM unnest(continuous_columns_null) AS x
    ) || (
        SELECT array_agg(x || ' IS NULL')
        FROM unnest(categorical_columns_null) AS x
    ) INTO tmp_array;
    
    
    
    query := 'INSERT INTO ' || output_table_name ||
             ' SELECT ' || array_to_string(tmp_array, ', ') ||
             ' FROM ' || input_table_name;
    RAISE DEBUG '%', query;

    start_ts := clock_timestamp();
    EXECUTE query;
    end_ts := clock_timestamp();
    RAISE INFO 'INSERT INTO TABLE WITH MISSING VALUES: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
    END$$;
    """
    #session.begin()
    engine2.execute(sqlalchemy.text(qry))
    #engine2.execute(qry)
    #session.commit()
    
    #connection = engine2.raw_connection()
    #try:
    #cursor = connection.cursor()
    #cursor.callproc
    print("creating new table...")
    engine2.execute("CALL generate_output_table('join_table', 'join_table_complete', ARRAY['AQI', 'CO', 'O3', 'PM10', 'PM2_5', 'NO2', 'NOx', 'NO_', 'WindSpeed', 'WindDirec', 'SO2_AVG'], ARRAY[]::text[], ARRAY['CO', 'O3', 'PM10', 'PM2_5', 'NO2', 'NOx', 'NO_', 'WindSpeed', 'WindDirec', 'SO2_AVG']::text[], ARRAY[]::text[])")
    print("done")
    #results = list(cursor.fetchall())
    #print(results)
    #cursor.close()
    #connection.commit()
    #finally:
    #connection.close()


#create_join_tables(engine2)

try:
	with engine.begin() as conn:
	    qry = 'CREATE DATABASE example_db WITH ENGINE = "postgres", PARAMETERS = {"user": "'+user_postgres+'","password": "","host": "localhost","port": "5432","database": "postgres"};'
	    resultset = conn.execute(qry)
except:
  print("Connection already established") 

#create_join_tables(session, engine2)
mice_iter = 1

col_nulls = ['co', 'o3', 'pm10', 'pm2_5', 'no2', 'nox', 'no_', 'windspeed', 'winddirec', 'so2_avg'];
#with engine.begin() as conn:
    #conn.execute("DROP PREDICTOR mindsdb.co")
    #conn.execute()

import time

t_ = time.time()
for i in range(mice_iter):
    for j in range(len(col_nulls)):
        t1 = time.time()
        print(col_nulls[j])
        qry = "CREATE PREDICTOR mindsdb."+col_nulls[j]+" FROM example_db (SELECT * FROM join_table_complete WHERE join_table_complete."+col_nulls[j]+"_ISNULL is false) PREDICT "+col_nulls[j]+\
        " USING model.args={\"submodels\":[{\"module\": \"LightGBM\", \"args\": { \"stop_after\": 12, \"fit_on_dev\": true}}]};"
        #qry = "CREATE PREDICTOR mindsdb."+col_nulls[j]+" FROM example_db (SELECT * FROM join_table_complete WHERE join_table_complete."+col_nulls[j]+"_ISNULL is false) PREDICT "+col_nulls[j]+\
        #" USING model.args={\"submodels\":[{\"module\": \"Regression\"}]};"
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
                
        qry = "UPDATE example_db.join_table_complete SET "+col_nulls[j]+" = source.prediction FROM ( "\
          "SELECT t.row_id, p."+col_nulls[j]+" as prediction "\
          "FROM example_db.join_table_complete as t "\
          "JOIN mindsdb."+col_nulls[j]+" as p WHERE t."+col_nulls[j]+"_isnull is true ) AS source WHERE row_id = source.row_id "
        with engine.begin() as conn:
            print(qry)
            resultset = conn.execute(qry)
            
        qry = "DROP PREDICTOR mindsdb."+col_nulls[j]+";"
        with engine.begin() as conn:
            resultset = conn.execute(qry)
        print("TIME: ", time.time()-t1)

print("TIME FULL: ", time.time()-t_)

with engine.begin() as conn:
    print(qry)
    resultset = conn.execute(qry)

