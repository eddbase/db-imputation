\pset pager off 
-- \pset tuples_only

DROP TABLE IF EXISTS Test1;
CREATE TABLE Test1(A int, B int);
INSERT INTO Test1 VALUES (2, 35);
INSERT INTO Test1 VALUES (4, 12);
INSERT INTO Test1 VALUES (3, 1);
INSERT INTO Test1 VALUES (4, 3);

SELECT to_cofactor(ARRAY[A,B], ARRAY[]::int4[]) AS "cofactor([A,B], [])" FROM Test1;
SELECT to_cofactor(ARRAY[A,B], ARRAY[A,B]) AS "cofactor([A,B], [A,B])" FROM Test1;

SELECT cat_to_cofactor(A) AS "cat(A)", B FROM Test1;
SELECT cat_to_cofactor(A) + cat_to_cofactor(B) AS "cat(A)+cat(B)" FROM Test1;
SELECT cat_to_cofactor(A) * cat_to_cofactor(B) AS "cat(B)*cat(B)" FROM Test1;
--SELECT lift2_cat(A, B) FROM Test1;
SELECT cat_to_cofactor(A) * cat_to_cofactor(B) * cat_to_cofactor(B) AS "cat(A)*cat(B)*cat(B)" FROM Test1;
--SELECT cat_to_cofactor(A) * cat_to_cofactor(B) * lift2_cat(A,B) AS rproduct FROM Test1;
SELECT SUM(cat_to_cofactor(A)) AS "sum(cat(A))" FROM Test1;
SELECT A, SUM(cat_to_cofactor(B)) AS "sum(cat(B))" FROM Test1 GROUP BY A;

SELECT cont_to_cofactor(A) AS "cont(A)", B FROM Test1;
SELECT cont_to_cofactor(A) * cont_to_cofactor(B) AS "cont(A)*cont(B)", A, B FROM Test1;
SELECT SUM(cont_to_cofactor(A) * cont_to_cofactor(B)) AS "sum(cont(A)*cont(B))" FROM Test1;
SELECT A, SUM(cont_to_cofactor(A) * cont_to_cofactor(B)) AS "sum(cont(A)*cont(B))" FROM Test1 GROUP BY A;
-- SELECT lift4_cont(A,B,B,A) FROM Test1;

SELECT cont_to_cofactor(A) * cat_to_cofactor(B) AS "cont(A)*cat(B)", B FROM Test1;
SELECT cont_to_cofactor(A) * cat_to_cofactor(B) * cont_to_cofactor(B) * cat_to_cofactor(A) AS "cont(A)*cat(B)*cont(B)*cat(A)" FROM Test1;
SELECT SUM(cont_to_cofactor(A) * cat_to_cofactor(B)) AS "sum(cont(A)*cat(B))" FROM Test1;

SELECT cont_to_cofactor(A) * cat_to_cofactor(B) * const_to_cofactor(333) AS "sum(cont(A)*cat(B)*const(333))" FROM Test1;
