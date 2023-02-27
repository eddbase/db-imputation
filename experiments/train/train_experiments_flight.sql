CREATE OR REPLACE PROCEDURE train_madlib_flight(
        continuous_columns text[],
        categorical_columns text[]
    ) LANGUAGE plpgsql AS $$
DECLARE
    query text;
    tbl text;
    cat_columns_names text[];
    start_ts timestamptz;
    end_ts   timestamptz;

BEGIN
    -- COMPUTE COLUMN AVERAGES (over a subset)
    query := 'DROP TABLE IF EXISTS tbl_madlib_flight';
    EXECUTE query;

    query := 'CREATE UNLOGGED TABLE tbl_madlib_flight AS ' ||
             '(SELECT ACTUAL_ELAPSED_TIME,' || array_to_string(continuous_columns || categorical_columns, ', ') || ' FROM flight.Route JOIN flight.schedule USING (ROUTE_ID) JOIN flight.flight USING (SCHEDULE_ID))';

    RAISE NOTICE '%', query;
    start_ts := clock_timestamp();
    EXECUTE query;
    end_ts := clock_timestamp();
    RAISE INFO 'TIME JOIN: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));


    tbl := 'tbl_madlib_flight';

    --if categorical values
    IF array_length(categorical_columns, 1) > 0 THEN
        query := 'SELECT madlib.encode_categorical_variables(''tbl_madlib_flight'', ''tbl_madlib_flight_1_hot'',''' ||
                array_to_string(categorical_columns, ', ') || ''');';
        RAISE NOTICE '%', query;
        start_ts := clock_timestamp();
        EXECUTE query;
        end_ts := clock_timestamp();
        RAISE INFO 'TIME 1-hot: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
        tbl := 'tbl_madlib_flight_1_hot';

        query := 'SELECT array_agg(quote_ident(column_name)) FROM information_schema.columns WHERE table_name = ''tbl_madlib_flight_1_hot''';
        RAISE NOTICE '%', query;
        EXECUTE query INTO cat_columns_names;
        continuous_columns := cat_columns_names;
    end if;
    ---continuous_columns has also 1-hot encoded feats
    query := 'SELECT madlib.linregr_train( ''' || tbl || ''', ''res_linregr'' ,''' ||
                             'ACTUAL_ELAPSED_TIME' || ''', ''ARRAY[' || array_to_string(continuous_columns, ', ') || ']'')';

    RAISE NOTICE '%', query;
    start_ts := clock_timestamp();
    EXECUTE query;
    end_ts := clock_timestamp();
    RAISE INFO 'TIME train: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

    IF array_length(categorical_columns, 1) > 0 THEN
        EXECUTE 'DROP TABLE tbl_madlib_flight_1_hot';
    end if;
    EXECUTE 'DROP TABLE tbl_madlib_flight';
    EXECUTE 'DROP TABLE res_linregr_summary';
END$$;
--'DIVERTED', 'EXTRA_DAY_ARR', 'EXTRA_DAY_DEP', 'OP_CARRIER'
CALL train_madlib_flight(ARRAY['CRS_DEP_HOUR', 'CRS_DEP_MIN', 'CRS_ARR_HOUR', 'CRS_ARR_MIN', 'DISTANCE', 'DEP_DELAY', 'TAXI_OUT', 'TAXI_IN', 'ARR_DELAY', 'AIR_TIME', 'DEP_TIME_HOUR', 'DEP_TIME_MIN', 'WHEELS_OFF_HOUR', 'WHEELS_OFF_MIN', 'WHEELS_ON_HOUR', 'WHEELS_ON_MIN', 'ARR_TIME_HOUR', 'ARR_TIME_MIN', 'MONTH_SIN', 'MONTH_COS', 'DAY_SIN', 'DAY_COS', 'WEEKDAY_SIN', 'WEEKDAY_COS'], ARRAY[]::text[]);

CREATE OR REPLACE PROCEDURE train_postgres_retailer_mat(
        continuous_columns text[],
        categorical_columns text[]
    ) LANGUAGE plpgsql AS $$
DECLARE
    query text;
    tbl text;
    cat_columns_names text[];
    start_ts timestamptz;
    end_ts   timestamptz;
    i int;
    j int;
    values float4[];
    params float8[];
BEGIN
    -- COMPUTE COLUMN AVERAGES (over a subset)
    query := 'DROP TABLE IF EXISTS tbl_postgres_flight';
    EXECUTE query;
    
        query := 'CREATE UNLOGGED TABLE tbl_postgres_flight AS ' ||
             '(SELECT * FROM flight.Route JOIN flight.schedule USING (ROUTE_ID) JOIN flight.flight USING (SCHEDULE_ID))';
             

    RAISE NOTICE '%', query;
    start_ts := clock_timestamp();
    EXECUTE query;
    end_ts := clock_timestamp();
    RAISE INFO 'TIME JOIN: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));


    tbl := 'tbl_postgres_flight';

    --if categorical values
    IF array_length(categorical_columns, 1) > 0 THEN
        query := 'SELECT madlib.encode_categorical_variables(''tbl_postgres_flight'', ''tbl_postgres_flight_1_hot'', ''' ||
                   array_to_string(categorical_columns, ', ') || ''');';
        RAISE NOTICE '%', query;
        start_ts := clock_timestamp();
        EXECUTE query;
        end_ts := clock_timestamp();
        RAISE INFO 'TIME 1-hot: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
        tbl := 'tbl_postgres_flight_1_hot';

        query := 'SELECT array_agg(quote_ident(column_name)) FROM information_schema.columns WHERE table_name = ''tbl_postgres_flight_1_hot''';
        RAISE NOTICE '%', query;
        EXECUTE query INTO cat_columns_names;
        continuous_columns := cat_columns_names;
        tbl := 'tbl_postgres_flight_1_hot';

    end if;

    RAISE NOTICE '%', continuous_columns;
    continuous_columns := array_prepend('1', continuous_columns);
    RAISE NOTICE '%', continuous_columns;

    query := '';
    FOR i in 1..array_length(continuous_columns, 1) LOOP
        FOR j in i..array_length(continuous_columns, 1) LOOP
            query := query || ' SUM(' || continuous_columns[i] || '*' || continuous_columns[j] || '), ';
        end loop;
    end loop;

    query := 'SELECT ARRAY[' || rtrim(query, ', ') || '] FROM ' || tbl;
    RAISE NOTICE '%', query;
    start_ts := clock_timestamp();
    EXECUTE query INTO values;
    RAISE NOTICE '%', values;
    end_ts := clock_timestamp();
    RAISE INFO 'TIME aggregates: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

    start_ts := clock_timestamp();
    params := ridge_linear_regression_from_params(values, 0, 0.001, 0, 10000);
    end_ts := clock_timestamp();
    RAISE INFO 'TIME train: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
    
    IF array_length(categorical_columns, 1) > 0 THEN
        EXECUTE 'DROP TABLE tbl_postgres_flight_1_hot';
    end if;
    EXECUTE 'DROP TABLE tbl_postgres_flight';
    EXECUTE 'DROP TABLE res_linregr_summary';

END$$;
--

CALL train_postgres_retailer_mat(ARRAY['CRS_DEP_HOUR', 'CRS_DEP_MIN', 'CRS_ARR_HOUR', 'CRS_ARR_MIN', 'DISTANCE', 'DEP_DELAY', 'TAXI_OUT', 'TAXI_IN', 'ARR_DELAY', 'AIR_TIME', 'DEP_TIME_HOUR', 'DEP_TIME_MIN', 'WHEELS_OFF_HOUR', 'WHEELS_OFF_MIN', 'WHEELS_ON_HOUR', 'WHEELS_ON_MIN', 'ARR_TIME_HOUR', 'ARR_TIME_MIN', 'MONTH_SIN', 'MONTH_COS', 'DAY_SIN', 'DAY_COS', 'WEEKDAY_SIN', 'WEEKDAY_COS'], ARRAY[]::text[]);

CREATE OR REPLACE PROCEDURE train_postgres_retailer(
        continuous_columns text[],
        categorical_columns text[]
    ) LANGUAGE plpgsql AS $$
DECLARE
    query text;
    tbl text;
    cat_columns_names text[];
    start_ts timestamptz;
    end_ts   timestamptz;
    i int;
    j int;
    values float4[];
    params float8[];
BEGIN
    -- COMPUTE COLUMN AVERAGES (over a subset)
    query := 'DROP TABLE IF EXISTS tbl_postgres_flight';
    EXECUTE query;
    
    
    tbl := ' flight.Route JOIN flight.schedule USING (ROUTE_ID) JOIN flight.flight USING (SCHEDULE_ID)';

    --if categorical values
    IF array_length(categorical_columns, 1) > 0 THEN
        query := 'SELECT madlib.encode_categorical_variables(''tbl_postgres_flight'', ''tbl_postgres_flight_1_hot'', ''' ||
                   array_to_string(categorical_columns, ', ') || ''');';
        RAISE NOTICE '%', query;
        start_ts := clock_timestamp();
        EXECUTE query;
        end_ts := clock_timestamp();
        RAISE INFO 'TIME 1-hot: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
        tbl := 'tbl_postgres_flight_1_hot';

        query := 'SELECT array_agg(quote_ident(column_name)) FROM information_schema.columns WHERE table_name = ''tbl_postgres_flight_1_hot''';
        RAISE NOTICE '%', query;
        EXECUTE query INTO cat_columns_names;
        continuous_columns := cat_columns_names;
        tbl := 'tbl_postgres_flight_1_hot';

    end if;

    RAISE NOTICE '%', continuous_columns;
    continuous_columns := array_prepend('1', continuous_columns);
    RAISE NOTICE '%', continuous_columns;

    query := '';
    FOR i in 1..array_length(continuous_columns, 1) LOOP
        FOR j in i..array_length(continuous_columns, 1) LOOP
            query := query || ' SUM(' || continuous_columns[i] || '*' || continuous_columns[j] || '), ';
        end loop;
    end loop;

    query := 'SELECT ARRAY[' || rtrim(query, ', ') || '] FROM ' || tbl;
    RAISE NOTICE '%', query;
    start_ts := clock_timestamp();
    EXECUTE query INTO values;
    RAISE NOTICE '%', values;
    end_ts := clock_timestamp();
    RAISE INFO 'TIME aggregates: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

    start_ts := clock_timestamp();
    params := ridge_linear_regression_from_params(values, 0, 0.001, 0, 10000);
    end_ts := clock_timestamp();
    RAISE INFO 'TIME train: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
    
    IF array_length(categorical_columns, 1) > 0 THEN
        EXECUTE 'DROP TABLE tbl_postgres_flight_1_hot';
    end if;
    EXECUTE 'DROP TABLE IF EXISTS tbl_postgres_flight';
    EXECUTE 'DROP TABLE IF EXISTS res_linregr_summary';

END$$;

CALL train_postgres_retailer(ARRAY['CRS_DEP_HOUR', 'CRS_DEP_MIN', 'CRS_ARR_HOUR', 'CRS_ARR_MIN', 'DISTANCE', 'DEP_DELAY', 'TAXI_OUT', 'TAXI_IN', 'ARR_DELAY', 'AIR_TIME', 'DEP_TIME_HOUR', 'DEP_TIME_MIN', 'WHEELS_OFF_HOUR', 'WHEELS_OFF_MIN', 'WHEELS_ON_HOUR', 'WHEELS_ON_MIN', 'ARR_TIME_HOUR', 'ARR_TIME_MIN', 'MONTH_SIN', 'MONTH_COS', 'DAY_SIN', 'DAY_COS', 'WEEKDAY_SIN', 'WEEKDAY_COS'], ARRAY[]::text[]);

CREATE OR REPLACE PROCEDURE train_cofactor_join_retailer_mat(
        continuous_columns text[],
        categorical_columns text[]
    ) LANGUAGE plpgsql AS $$
DECLARE
    query text;
    tbl text;
    cat_columns_names text[];
    start_ts timestamptz;
    end_ts   timestamptz;
    i int;
    j int;
    cofactor cofactor;
    label_index int;
    params float[];
BEGIN
    -- COMPUTE COLUMN AVERAGES (over a subset)
    query := 'DROP TABLE IF EXISTS tbl_postgres_flight';
    EXECUTE query;

        query := 'CREATE UNLOGGED TABLE tbl_postgres_flight AS ' ||
             '(SELECT * FROM flight.Route JOIN flight.schedule USING (ROUTE_ID) JOIN flight.flight USING (SCHEDULE_ID))';

    RAISE NOTICE '%', query;
    start_ts := clock_timestamp();
    EXECUTE query;
    end_ts := clock_timestamp();
    RAISE INFO 'TIME JOIN: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));


    tbl := 'tbl_postgres_flight';

    query := 'SELECT SUM(to_cofactor(ARRAY[' || array_to_string(continuous_columns, ', ') || ']::float8[], ARRAY[' ||
             array_to_string(categorical_columns, ', ') || ']::int4[])) FROM tbl_postgres_flight';

    RAISE NOTICE '%', query;
    start_ts := clock_timestamp();
    EXECUTE query INTO cofactor;
    end_ts := clock_timestamp();
    RAISE INFO 'TIME cofactor: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

    label_index := array_position(continuous_columns, 'inventoryunits');
    start_ts := clock_timestamp();
    params := ridge_linear_regression(cofactor, label_index - 1, 0.001, 0, 10000);
    end_ts := clock_timestamp();
    RAISE INFO 'TIME train: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

END$$;

CALL train_cofactor_join_retailer_mat(ARRAY['CRS_DEP_HOUR', 'CRS_DEP_MIN', 'CRS_ARR_HOUR', 'CRS_ARR_MIN', 'DISTANCE', 'DEP_DELAY', 'TAXI_OUT', 'TAXI_IN', 'ARR_DELAY', 'AIR_TIME', 'DEP_TIME_HOUR', 'DEP_TIME_MIN', 'WHEELS_OFF_HOUR', 'WHEELS_OFF_MIN', 'WHEELS_ON_HOUR', 'WHEELS_ON_MIN', 'ARR_TIME_HOUR', 'ARR_TIME_MIN', 'MONTH_SIN', 'MONTH_COS', 'DAY_SIN', 'DAY_COS', 'WEEKDAY_SIN', 'WEEKDAY_COS'], ARRAY['DIVERTED', 'EXTRA_DAY_ARR', 'EXTRA_DAY_DEP', 'OP_CARRIER']::text[]);

---

CREATE OR REPLACE PROCEDURE train_cofactor_join_retailer(
        continuous_columns text[],
        categorical_columns text[]
    ) LANGUAGE plpgsql AS $$
DECLARE
    query text;
    tbl text;
    cat_columns_names text[];
    start_ts timestamptz;
    end_ts   timestamptz;
    i int;
    j int;
    cofactor cofactor;
    label_index int;
    params float[];
BEGIN
    -- COMPUTE COLUMN AVERAGES (over a subset)
    query := 'DROP TABLE IF EXISTS tbl_postgres_flight';
    EXECUTE query;
    
    
    query := 'SELECT SUM(to_cofactor(ARRAY[' || array_to_string(continuous_columns, ', ') || ']::float8[], ARRAY[' ||
             array_to_string(categorical_columns, ', ') || ']::int4[])) FROM flight.Route JOIN flight.schedule USING (ROUTE_ID) JOIN flight.flight USING (SCHEDULE_ID)';

    RAISE NOTICE '%', query;
    start_ts := clock_timestamp();
    EXECUTE query INTO cofactor;
    end_ts := clock_timestamp();
    RAISE INFO 'TIME cofactor: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

    label_index := array_position(continuous_columns, 'inventoryunits');
    start_ts := clock_timestamp();
    params := ridge_linear_regression(cofactor, label_index - 1, 0.001, 0, 10000);
    end_ts := clock_timestamp();
    RAISE INFO 'TIME train: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));

END$$;

CALL train_cofactor_join_retailer(ARRAY['CRS_DEP_HOUR', 'CRS_DEP_MIN', 'CRS_ARR_HOUR', 'CRS_ARR_MIN', 'DISTANCE', 'DEP_DELAY', 'TAXI_OUT', 'TAXI_IN', 'ARR_DELAY', 'AIR_TIME', 'DEP_TIME_HOUR', 'DEP_TIME_MIN', 'WHEELS_OFF_HOUR', 'WHEELS_OFF_MIN', 'WHEELS_ON_HOUR', 'WHEELS_ON_MIN', 'ARR_TIME_HOUR', 'ARR_TIME_MIN', 'MONTH_SIN', 'MONTH_COS', 'DAY_SIN', 'DAY_COS', 'WEEKDAY_SIN', 'WEEKDAY_COS'], ARRAY['DIVERTED', 'EXTRA_DAY_ARR', 'EXTRA_DAY_DEP', 'OP_CARRIER']::text[]);

CREATE OR REPLACE PROCEDURE train_cofactor_factorized_flight() LANGUAGE plpgsql AS $$
DECLARE
    query text;
    tbl text;
    cat_columns_names text[];
    start_ts timestamptz;
    end_ts   timestamptz;
    i int;
    j int;
    cofactor cofactor;
    label_index int;
    params float[];
BEGIN
	query := 'SELECT SUM(to_cofactor(ARRAY[DEP_DELAY, TAXI_OUT, TAXI_IN, ARR_DELAY, ACTUAL_ELAPSED_TIME, AIR_TIME, DEP_TIME_HOUR, DEP_TIME_MIN, WHEELS_OFF_HOUR, WHEELS_OFF_MIN, WHEELS_ON_HOUR, WHEELS_ON_MIN, ARR_TIME_HOUR, ARR_TIME_MIN, MONTH_SIN, MONTH_COS, DAY_SIN, DAY_COS, WEEKDAY_SIN, WEEKDAY_COS], ARRAY[]::int4[])*cnt1)
	FROM flight.flight as flight JOIN (
    	SELECT SCHEDULE_ID, to_cofactor(ARRAY[CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN], ARRAY[]::int4[])*cnt as cnt1
    	FROM flight.schedule as schedule JOIN 
    	(SELECT
    		ROUTE_ID, to_cofactor(ARRAY[DISTANCE], ARRAY[]::int4[]) as cnt
    	FROM
      	flight.route) as route
      	ON route.ROUTE_ID = schedule.ROUTE_ID) as schedule ON flight.SCHEDULE_ID = schedule.SCHEDULE_ID;';
      
    --RAISE NOTICE '%', query;
    start_ts := clock_timestamp();
    EXECUTE query INTO cofactor;
    end_ts := clock_timestamp();
    RAISE INFO 'TIME cofactor: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
    label_index := 0;
    start_ts := clock_timestamp();
    params := ridge_linear_regression(cofactor, label_index, 0.001, 0, 10000);
    end_ts := clock_timestamp();
    RAISE INFO 'TIME train: ms = %', 1000 * (extract(epoch FROM end_ts - start_ts));
END$$;

CALL train_cofactor_factorized_flight();
