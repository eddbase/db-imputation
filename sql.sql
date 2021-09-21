CREATE TYPE triple;

CREATE OR REPLACE FUNCTION triple_in(cstring)
    RETURNS triple
    AS '/mnt/c/Users/massi/phd/cmake-build-debug/libphd.so'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION triple_out(triple)
    RETURNS cstring
    AS '/mnt/c/Users/massi/phd/cmake-build-debug/libphd.so'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION triple_recv(internal)
   RETURNS triple
   AS '/mnt/c/Users/massi/phd/cmake-build-debug/libphd.so'
   LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION triple_send(triple)
   RETURNS bytea
   AS '/mnt/c/Users/massi/phd/cmake-build-debug/libphd.so'
   LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE triple (
   internallength = 16,
   input = triple_in,
   output = triple_out,
   receive = triple_recv,
   send = triple_send,
   alignment = double
);

CREATE OR REPLACE FUNCTION triple_add(triple, triple)
    RETURNS triple
    AS '/mnt/c/Users/massi/phd/cmake-build-debug/libphd.so', 'triple_add'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE OPERATOR + (
    leftarg = triple,
    rightarg = triple,
    procedure = triple_add,
    commutator = +
);

CREATE AGGREGATE sum (triple)
(
    sfunc = triple_add,
    stype = triple,
    initcond = '(0,0)'
);

CREATE OR REPLACE FUNCTION lift(i float) RETURNS triple AS $$
        BEGIN
                RETURN concat('(', 1, ', ', i, ')');
        END;
$$ LANGUAGE plpgsql;

DROP TABLE Test;

CREATE TABLE Test (
    A int,
    B float
);

insert into Test VALUES (1,2),(3,4),(5,6),(7,8),(9,10);

SELECT SUM(lift(A)) FROM Test;