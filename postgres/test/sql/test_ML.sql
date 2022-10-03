\pset pager off
-- SET client_min_messages TO DEBUG4;

DROP TABLE IF EXISTS Test1;
CREATE TABLE Test1(A int, B int, C int, D int);
INSERT INTO Test1 VALUES (2, 35, 1, 4);
INSERT INTO Test1 VALUES (4, 12, 2, 9);
INSERT INTO Test1 VALUES (3, 1, 43, 0);
INSERT INTO Test1 VALUES (4, 3, 6, 9);

DO $$
DECLARE 
    cofactor_global cofactor;
    params FLOAT[];
BEGIN
    SELECT SUM(to_cofactor(ARRAY[A,B,C,D], ARRAY[]::int4[])) AS "cofactor" INTO STRICT cofactor_global FROM Test1;
    params := ridge_linear_regression(cofactor_global, 0, 0.001, 0, 10000);

    SELECT SUM(to_cofactor(ARRAY[A,B,C,D], ARRAY[]::int4[])) AS "cofactor" INTO STRICT cofactor_global FROM Test1;
    params := ridge_linear_regression(cofactor_global, 0, 0.001, 0.01, 10000);

    SELECT SUM(to_cofactor(ARRAY[A,B,C,D], ARRAY[]::int4[])) AS "cofactor" INTO STRICT cofactor_global FROM Test1;
    params := ridge_linear_regression(cofactor_global, 1, 0.001, 0, 10000);

    SELECT SUM(to_cofactor(ARRAY[A,B], ARRAY[C,D])) AS "cofactor" INTO STRICT cofactor_global FROM Test1;
    params := ridge_linear_regression(cofactor_global, 0, 0.001, 0, 10000);

    
    SELECT SUM(cont_to_cofactor(A) * cat_to_cofactor(B) * cat_to_cofactor(C) * cat_to_cofactor(D)) AS "cofactor" INTO STRICT cofactor_global FROM Test1;
    params := ridge_linear_regression(cofactor_global, 0, 0.001, 0, 10000);

    SELECT SUM(to_cofactor(ARRAY[A], ARRAY[B,C,D])) AS "cofactor" INTO STRICT cofactor_global FROM Test1;
    params := ridge_linear_regression(cofactor_global, 0, 0.001, 0, 10000);

    -- issues a warning
    SELECT SUM(to_cofactor(ARRAY[]::float8[], ARRAY[B,C,D])) AS "cofactor" INTO STRICT cofactor_global FROM Test1;
    params := ridge_linear_regression(cofactor_global, 0, 0.001, 0, 10000);
END$$;

