--SET client_min_messages TO WARNING;

LOAD :FACTML_LIBRARY;

DROP TYPE cofactor CASCADE;
CREATE TYPE cofactor;

CREATE OR REPLACE FUNCTION read_cofactor(cstring)
    RETURNS cofactor
    AS :FACTML_LIBRARY, 'read_cofactor'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION write_cofactor(cofactor)
    RETURNS cstring
    AS :FACTML_LIBRARY, 'write_cofactor'
    LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE cofactor (
   internallength = VARIABLE,
   input = read_cofactor,
   output = write_cofactor,
   alignment = double
);

CREATE OR REPLACE FUNCTION add_cofactors(cofactor, cofactor)
    RETURNS cofactor
    AS :FACTML_LIBRARY, 'pg_add_cofactors'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION sub_cofactors(cofactor, cofactor)
    RETURNS cofactor
    AS :FACTML_LIBRARY, 'pg_subtract_cofactors'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION mul_cofactors(cofactor, cofactor)
    RETURNS cofactor
    AS :FACTML_LIBRARY, 'pg_multiply_cofactors'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR + (
    leftarg = cofactor,
    rightarg = cofactor,
    procedure = add_cofactors,
    commutator = +
);

CREATE OPERATOR - (
    leftarg = cofactor,
    rightarg = cofactor,
    procedure = sub_cofactors
);

CREATE OPERATOR * (
    leftarg = cofactor,
    rightarg = cofactor,
    procedure = mul_cofactors,
    commutator = *
);

CREATE AGGREGATE sum (cofactor) (
    sfunc = add_cofactors,
    stype = cofactor,
    COMBINEFUNC = add_cofactors,
    PARALLEL = SAFE
);

CREATE OR REPLACE FUNCTION const_to_cofactor(i int4)
    RETURNS cofactor
    AS :FACTML_LIBRARY, 'lift_const_to_cofactor'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION cont_to_cofactor(i float8)
    RETURNS cofactor
    AS :FACTML_LIBRARY, 'lift_cont_to_cofactor'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION cat_to_cofactor(i int4)
    RETURNS cofactor
    AS :FACTML_LIBRARY, 'lift_cat_to_cofactor'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION to_cofactor(cont_vals float8[], cat_vals int4[])
    RETURNS cofactor
    AS :FACTML_LIBRARY, 'lift_to_cofactor'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE PROCEDURE cofactor_stats(cofactor)
    AS :FACTML_LIBRARY, 'pg_cofactor_stats'
    LANGUAGE C;
