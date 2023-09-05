CREATE OR REPLACE PROCEDURE NB() LANGUAGE plpgsql AS $$
DECLARE 
	aggregates nb_aggregates[];--categorical columns sorted by values  -> FLIGHTS
	params float4[];
	labels int[];
	pred int;
BEGIN
    
    select array_agg(label), array_agg(aggregate) INTO labels, aggregates from (
    select target as label, SUM(to_nb_aggregates(ARRAY[a,b,c,d], ARRAY[]::integer[])) as aggregate from iris group by target) as x;
    
    RAISE INFO 'aggregates % ', aggregates;
    
	params := naive_bayes_train(aggregates);
	
	---params are: n. classes to predict, n. categorical columns, number of categories in each column (mon. increasing),
	---sorted classes in each categorical column, prior for each class to predict, for each predict. class: mean and variance for each
	--- num. attribute, then prob cat. attribute assumes a value 
	RAISE INFO 'params % labels % ', params, labels;
	
	
	
	pred := naive_bayes_predict(params, ARRAY[5.0, 3.2, 1.3, 0.2], ARRAY[]::integer[]);
	RAISE INFO 'prediction %', labels[pred+1];--postgres array starts from 1
	pred := naive_bayes_predict(params, ARRAY[6.2, 2.2, 4.5, 1.5], ARRAY[]::integer[]);
	RAISE INFO 'prediction %', labels[pred+1];--postgres array starts from 1
	
	---
	
	select array_agg(label), array_agg(aggregate) INTO labels, aggregates from (
    select target as label, SUM(to_nb_aggregates(ARRAY[a,b], ARRAY[c,d]::integer[])) as aggregate from synthetic group by target) as x;
    
    RAISE INFO 'aggregates % ', aggregates;
	params := naive_bayes_train(aggregates);
	RAISE INFO 'params % labels % ', params, labels;
		
	pred := naive_bayes_predict(params, ARRAY[-1.056512, 0.709194], ARRAY[6, 2]::integer[]);
	RAISE INFO 'prediction %', labels[pred+1];--postgres array starts from 1
	pred := naive_bayes_predict(params, ARRAY[4.648401, 3.105455], ARRAY[8, 2]::integer[]);
	RAISE INFO 'prediction %', labels[pred+1];--postgres array starts from 1

END$$;

CALL NB();
