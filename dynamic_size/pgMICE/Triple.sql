LOAD '/mnt/c/Users/massi/phd/dynamic_size/cmake-build-debug/libfactMICE.so';
DROP TYPE triple CASCADE;

CREATE TYPE triple;

CREATE OR REPLACE FUNCTION triple_in(cstring)
    RETURNS triple
    AS '/mnt/c/Users/massi/phd/dynamic_size/cmake-build-debug/libfactMICE.so'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION triple_out(triple)
    RETURNS cstring
    AS '/mnt/c/Users/massi/phd/dynamic_size/cmake-build-debug/libfactMICE.so'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION triple_recv(internal)
   RETURNS triple
   AS '/mnt/c/Users/massi/phd/dynamic_size/cmake-build-debug/libfactMICE.so'
   LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION triple_send(triple)
   RETURNS bytea
   AS '/mnt/c/Users/massi/phd/dynamic_size/cmake-build-debug/libfactMICE.so'
   LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE triple (
   internallength = VARIABLE,
   input = triple_in,
   output = triple_out,
   receive = triple_recv,
   send = triple_send,
   alignment = double
);

CREATE OR REPLACE FUNCTION triple_add(triple, triple)
    RETURNS triple
    AS '/mnt/c/Users/massi/phd/dynamic_size/cmake-build-debug/libfactMICE.so', 'triple_add'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION triple_sub(triple, triple)
    RETURNS triple
    AS '/mnt/c/Users/massi/phd/dynamic_size/cmake-build-debug/libfactMICE.so', 'triple_sub'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION triple_mul(triple, triple)
    RETURNS triple
    AS '/mnt/c/Users/massi/phd/dynamic_size/cmake-build-debug/libfactMICE.so', 'triple_mul'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR + (
    leftarg = triple,
    rightarg = triple,
    procedure = triple_add,
    commutator = +
);

CREATE OPERATOR - (
    leftarg = triple,
    rightarg = triple,
    procedure = triple_sub,
    commutator = -
);

CREATE OPERATOR * (
    leftarg = triple,
    rightarg = triple,
    procedure = triple_mul,
    commutator = *
);

CREATE AGGREGATE sum (triple)
(
    sfunc = triple_add,
    stype = triple,
	COMBINEFUNC = triple_add,
	PARALLEL = SAFE
);

CREATE OR REPLACE FUNCTION cofactor_send(triple Triple)
    returns float8[] as '/mnt/c/Users/massi/phd/dynamic_size/cmake-build-debug/libfactMICE.so', 'cofactor_send'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;


CREATE OR REPLACE FUNCTION liftpgsql(i float, j int) RETURNS triple AS $$
  DECLARE
    _arr float[];
    _matrix float[][];
  BEGIN
    _arr := array_fill(0, array[4]);
	_arr[j] := i;
	_matrix := array_fill(0, array[4,4]);
	_matrix[j][j] := i*i;
    RETURN concat('(', 1, ', [ ', array_to_string(_arr, ', '),' ], [ ', array_to_string(_matrix, ', '), ' ])');
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION lift_both(a float, b float, c float, d float) RETURNS triple AS $$
  DECLARE
    _arr float[];
    _matrix float[][];
  BEGIN
    _arr[1] := a;
    _arr[2] := b;
    _arr[3] := c;
    _arr[4] := d;
    _matrix := array_fill(0, array[4,4]);
    _matrix[1][1] := a*a;
    _matrix[2][2] := b*b;
    _matrix[3][3] := c*c;
    _matrix[4][4] := d*d;
    RETURN concat('(', 1, ', [ ', array_to_string(_arr, ', '), ' ], [ ', array_to_string(_matrix, ', '), ' ])');
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION liftConst(i int)
    RETURNS triple
    AS '/mnt/c/Users/massi/phd/dynamic_size/cmake-build-debug/libfactMICE.so', 'lift_const'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION lift(i float)
    RETURNS triple
    AS '/mnt/c/Users/massi/phd/dynamic_size/cmake-build-debug/libfactMICE.so', 'lift'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION lift2(i float, j float)
    RETURNS triple
    AS '/mnt/c/Users/massi/phd/dynamic_size/cmake-build-debug/libfactMICE.so', 'lift2'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION converge(triple triple, label_idx int, step_size float8, lambda float8, max_iterations int)
    RETURNS float8[]
    AS '/mnt/c/Users/massi/phd/dynamic_size/cmake-build-debug/libfactMICE.so', 'converge'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION update_triple(arr_updates float8[], triple triple, col_update int)
    RETURNS triple
    AS '/mnt/c/Users/massi/phd/dynamic_size/cmake-build-debug/libfactMICE.so', 'converge'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

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

--UPDATE inventory SET store = NULL WHERE id <= 4200000;
--UPDATE inventory SET date = NULL WHERE (id <= 8400000 AND id >= 4200000);
--UPDATE inventory SET item = NULL WHERE id <= 4200000;
--UPDATE inventory SET units = NULL WHERE id <= 4200000;

--4200000 5% 1 col
--
--4200000 5% 2 col
--4200000 5% 3 col
--4200000 5% 4 col

--8400000 10%
--16800000 20%
