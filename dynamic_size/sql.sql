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

CREATE OR REPLACE FUNCTION triple_mul(triple, triple)
    RETURNS triple
    AS '/mnt/c/Users/massi/phd/dynamic_size/cmake-build-debug/libfactMICE.so', 'triple_mul'
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
        RETURN concat('(', 1, ',[',array_to_string(_arr, ','),'],[',array_to_string(_matrix, ','),'])');
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
    RETURN concat('(', 1, ',[',array_to_string(_arr, ','),'],[',array_to_string(_matrix, ','),'])');
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION liftc(i float, j int, cols int)
    RETURNS triple
    AS '/mnt/c/Users/massi/phd/dynamic_size/cmake-build-debug/libfactMICE.so', 'lift'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

SELECT (liftc(a, 0, 2)) from Test1;
SELECT sum(liftc(a, 0, 4)) from Test1;


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

SELECT SUM(liftc(store, 0)) FROM Inventory;
