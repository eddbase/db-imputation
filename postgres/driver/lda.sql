CREATE OR REPLACE PROCEDURE LDA() LANGUAGE plpgsql AS $$
DECLARE 
	aggregates cofactor;--categorical columns sorted by values  -> FLIGHTS
	params float4[];
	pred int;
BEGIN
    
    select SUM(to_cofactor(ARRAY[a,b,c,d], ARRAY[target]::integer[])) from iris INTO aggregates;
    
    RAISE INFO 'aggregates % ', aggregates;
    
	params := lda_train(aggregates, 0, 0);
	
	RAISE INFO 'params % ', params;
	pred := lda_predict(params, ARRAY[5.0, 3.2, 1.3, 0.2], ARRAY[]::integer[]);
	RAISE INFO 'prediction %', pred;--postgres array starts from 1
	pred := lda_predict(params, ARRAY[6.2, 2.2, 4.5, 1.5], ARRAY[]::integer[]);
	RAISE INFO 'prediction %', pred;--postgres array starts from 1

    select SUM(to_cofactor(ARRAY[a,b], ARRAY[c,d, target]::integer[])) from synthetic INTO aggregates;
    
    RAISE INFO 'aggregates % ', aggregates;
	params := lda_train(aggregates, 2, 0);
	RAISE INFO 'params % ', params;
		
	pred := lda_predict(params, ARRAY[-1.056512, 0.709194], ARRAY[6, 2]::integer[]);--0
	RAISE INFO 'prediction %', pred;--postgres array starts from 1
	pred := lda_predict(params, ARRAY[4.648401, 3.105455], ARRAY[8, 2]::integer[]);--1
	RAISE INFO 'prediction %', pred;--postgres array starts from 1

END$$;

create table iris (a float, b float, c float, d float, target integer);
\copy iris FROM 'iris.csv' WITH (FORMAT CSV, NULL '');

create table synthetic (a float, b float, c float, d float, target integer);
\copy synthetic FROM 'synthetic.csv' WITH (FORMAT CSV, NULL '');


CALL LDA();
