CREATE OR REPLACE PROCEDURE NB() LANGUAGE plpgsql AS $$
DECLARE 
	aggregates nb_aggregates[];--categorical columns sorted by values  -> FLIGHTS
	params float4[];
	labels int[];
	pred int;
BEGIN
    
    select array_agg(label), array_agg(aggregate) INTO labels, aggregates from (
    select E as label, SUM(to_nb_aggregates(ARRAY[A, B], ARRAY[C, D])) as aggregate from test group by E) as x;
    
	params := naive_bayes_train(aggregates);
	RAISE INFO 'params %', params;
	pred := naive_bayes_predict(params, ARRAY[1,2], ARRAY[3,4]);
	RAISE INFO 'prediction %', labels[pred+1];--postgres array starts from 1
	
	pred := naive_bayes_predict(params, ARRAY[5,6], ARRAY[3,5]);
	RAISE INFO 'prediction %', labels[pred+1];--postgres array starts from 1


END$$;

create table test (a float, b float, c integer, d integer, e integer);

insert into test values(1,2,3,4,5);
insert into test values(2,3,4,5,5);
insert into test values(3,4,4,4,5);
insert into test values(5,6,3,5,3);
insert into test values(6,7,8,9,1);
insert into test values(7,8,9,10,1);

CALL NB();
