CREATE OR REPLACE PROCEDURE QDA() LANGUAGE plpgsql AS $$
DECLARE 
	aggregates cofactor[];--categorical columns sorted by values  -> FLIGHTS
	params float4[];
	labels int[];
	pred int;
BEGIN
    
    select array_agg(label), array_agg(aggregate) INTO labels, aggregates from (
    select target as label, SUM(to_cofactor(ARRAY[a,b,c,d], ARRAY[]::integer[])) as aggregate from iris group by target) as x;
    
    RAISE INFO 'aggregates % ', aggregates;
    
	params := qda_train(aggregates);
	--params are n_aggregates (labels), num_categorical_vars over columns, n. unique cat. values in each column, cat. values in each column
	--- X, x, x_0 for each class
	
	
	RAISE INFO 'params % labels % ', params, labels;
	pred := qda_predict(params, ARRAY[5.0, 3.2, 1.3, 0.2], ARRAY[]::integer[]);
	RAISE INFO 'prediction %', labels[pred+1];--postgres array starts from 1
	pred := qda_predict(params, ARRAY[6.2, 2.2, 4.5, 1.5], ARRAY[]::integer[]);
	RAISE INFO 'prediction %', labels[pred+1];--postgres array starts from 1

    select array_agg(label), array_agg(aggregate) INTO labels, aggregates from (
    select target as label, SUM(to_cofactor(ARRAY[a,b], ARRAY[c,d]::integer[])) as aggregate from synthetic group by target) as x;
    
    RAISE INFO 'aggregates % ', aggregates;
	params := qda_train(aggregates);
	RAISE INFO 'params % labels % ', params, labels;
		
	pred := qda_predict(params, ARRAY[-1.056512, 0.709194], ARRAY[6, 2]::integer[]);--0
	RAISE INFO 'index % prediction %', pred, labels[pred+1];--postgres array starts from 1
	pred := qda_predict(params, ARRAY[4.648401, 3.105455], ARRAY[8, 2]::integer[]);--1
	RAISE INFO 'index % prediction %', pred, labels[pred+1];--postgres array starts from 1

END$$;

create table iris (a float, b float, c float, d float, target integer);
\copy iris FROM 'iris.csv' WITH (FORMAT CSV, NULL '');

create table synthetic (a float, b float, c float, d float, target integer);
\copy synthetic FROM 'synthetic.csv' WITH (FORMAT CSV, NULL '');


CALL QDA();
