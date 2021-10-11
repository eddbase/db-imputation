LOAD '/Users/milllic/repo/work/imputation/dynamic_size/libfactMICE.dylib';
DROP TYPE triple CASCADE;

CREATE TYPE triple;

CREATE OR REPLACE FUNCTION triple_in(cstring)
    RETURNS triple
    AS '/Users/milllic/repo/work/imputation/dynamic_size/libfactMICE.dylib'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION triple_out(triple)
    RETURNS cstring
    AS '/Users/milllic/repo/work/imputation/dynamic_size/libfactMICE.dylib'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION triple_recv(internal)
   RETURNS triple
   AS '/Users/milllic/repo/work/imputation/dynamic_size/libfactMICE.dylib'
   LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION triple_send(triple)
   RETURNS bytea
   AS '/Users/milllic/repo/work/imputation/dynamic_size/libfactMICE.dylib'
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
    AS '/Users/milllic/repo/work/imputation/dynamic_size/libfactMICE.dylib', 'triple_add'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION triple_mul(triple, triple)
    RETURNS triple
    AS '/Users/milllic/repo/work/imputation/dynamic_size/libfactMICE.dylib', 'triple_mul'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR + (
    leftarg = triple,
    rightarg = triple,
    procedure = triple_add,
    commutator = +
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
    AS '/Users/milllic/repo/work/imputation/dynamic_size/libfactMICE.dylib', 'lift_const'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION lift(i float)
    RETURNS triple
    AS '/Users/milllic/repo/work/imputation/dynamic_size/libfactMICE.dylib', 'lift'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION lift2(i float, j float)
    RETURNS triple
    AS '/Users/milllic/repo/work/imputation/dynamic_size/libfactMICE.dylib', 'lift2'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;


DROP TABLE Test1;
DROP TABLE Test2;

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

insert into Test1 VALUES (1,2,3,4),(1,2,3,4),(1,2,3,4),(1,2,3,4);
insert into Test2 VALUES (1,2),(1,4),(5,6),(5,8),(9,10);

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
