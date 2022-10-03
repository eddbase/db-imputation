\pset pager off

DROP TABLE IF EXISTS Test1;
CREATE TABLE Test1(A int, B int);
INSERT INTO Test1 VALUES (2, 35);
INSERT INTO Test1 VALUES (4, 12);
INSERT INTO Test1 VALUES (3, 1);
INSERT INTO Test1 VALUES (4, 3);

SELECT to_relation(A) AS "rel(A)", B FROM Test1;
SELECT to_relation(A) + to_relation(B) AS "rel(A)+rel(B)" FROM Test1;
SELECT to_relation(A) * to_relation(B) AS "rel(A)*rel(B)" FROM Test1;
SELECT SUM(to_relation(A)) AS "sum(rel(A))" FROM Test1;
SELECT A, SUM(to_relation(B)) AS "sum(rel(B))" FROM Test1 GROUP BY A;
