
DROP TABLE Test1;
DROP TABLE Test2;
DROP TABLE Test3;
DROP TABLE Test4;

CREATE TABLE Test1 (
    A int,
    B float,
	C float,
	D float
);

CREATE TABLE Test2 (
    C int,
    D float
);

CREATE TABLE Test3 (
    pkey int,
    A int,
    B float,
	C float,
	D float
);

CREATE TABLE Test4 (
    pkey int,
    A int,
    B float
);

insert into Test1 VALUES (1,2,3,4),(1,2,3,4),(1,2,3,4),(1,2,3,4);
insert into Test2 VALUES (1,2),(1,4),(5,6),(5,8),(9,10);
insert into Test3 VALUES (1,1,NULL,NULL,4),(2,1,2,3,NULL),(3,1,2,3,4),(4,1,NULL,3,4);
insert into Test4 VALUES (1,1,NULL),(2,1,2),(3,1,2),(2,1,3),(3,1,3),(4,1,NULL);

-- SELECT SUM(lift(store, 0)) FROM Inventory;
SELECT (lift(a)) from Test1;
SELECT (lift2(a, b)) from Test1;
SELECT (liftConst(12)) from Test1;

SELECT sum(lift(a)) from Test1;
SELECT sum(lift2(a, b)) from Test1;
SELECT sum(lift(a) * lift(b)) from Test1;     -- same as previous
SELECT sum(lift(a) * lift(b) * lift2(c, d)) from Test1;

SELECT sum(lift2(c, d)) from Test2;
SELECT sum(lift(c) * lift(d)) from Test2; -- same as previous
SELECT sum(liftConst(1) * lift(c) * lift(d)) from Test2; -- same as previous
SELECT sum(liftConst(2) * lift(c) * lift(d)) from Test2;