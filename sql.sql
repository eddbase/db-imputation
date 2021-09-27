LOAD '/mnt/c/Users/massi/phd/cmake-build-debug/lib2.so';
DROP TYPE triple CASCADE;

CREATE TYPE triple;

CREATE OR REPLACE FUNCTION triple_in(cstring)
    RETURNS triple
    AS '/mnt/c/Users/massi/phd/cmake-build-debug/lib2.so'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION triple_out(triple)
    RETURNS cstring
    AS '/mnt/c/Users/massi/phd/cmake-build-debug/lib2.so'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION triple_recv(internal)
   RETURNS triple
   AS '/mnt/c/Users/massi/phd/cmake-build-debug/lib2.so'
   LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION triple_send(triple)
   RETURNS bytea
   AS '/mnt/c/Users/massi/phd/cmake-build-debug/lib2.so'
   LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE triple (
   internallength = 56,
   input = triple_in,
   output = triple_out,
   receive = triple_recv,
   send = triple_send,
   alignment = double
);

CREATE OR REPLACE FUNCTION triple_add(triple, triple)
    RETURNS triple
    AS '/mnt/c/Users/massi/phd/cmake-build-debug/lib2.so', 'triple_add'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION triple_mul(triple, triple)
    RETURNS triple
    AS '/mnt/c/Users/massi/phd/cmake-build-debug/lib2.so', 'triple_mul'
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
    initcond = '(0,[0,0],[0,0,0,0])'
);

CREATE OR REPLACE FUNCTION lift(i float, j int) RETURNS triple AS $$
DECLARE
_arr float[];
_matrix float[][];
BEGIN
	_arr[1] := 0;
	_arr[2] := 0;
	_arr[j] := i;
	_matrix := array_fill(0, array[2,2]);
	_matrix[j][j] := i*i;
        RETURN concat('(', 1, ',[',array_to_string(_arr, ','),'],[',array_to_string(_matrix, ','),'])');
    END;
$$ LANGUAGE plpgsql;


DROP TABLE Test1;
DROP TABLE Test2;

CREATE TABLE Test1 (
    A int,
    B float
);

CREATE TABLE Test2 (
    C int,
    D float
);

insert into Test1 VALUES (1,2),(3,4),(5,6),(7,8),(9,10);
insert into Test2 VALUES (1,2),(1,4),(5,6),(5,8),(9,10);


SELECT SUM(lift(A)) FROM Test;
SELECT lift(A, 1) FROM Test1;

SELECT SUM(lift(A, 1) * lift(B, 2)) FROM Test1;