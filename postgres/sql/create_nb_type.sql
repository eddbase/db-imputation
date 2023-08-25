--SET client_min_messages TO WARNING;

LOAD :FACTML_LIBRARY;

DROP TYPE nb_aggregates CASCADE;
CREATE TYPE nb_aggregates;

CREATE OR REPLACE FUNCTION read_nb_aggregates(cstring)
    RETURNS nb_aggregates
    AS :FACTML_LIBRARY, 'read_nb_aggregates'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION write_nb_aggregates(nb_aggregates)
    RETURNS cstring
    AS :FACTML_LIBRARY, 'write_nb_aggregates'
    LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE nb_aggregates (
   internallength = VARIABLE,
   input = read_nb_aggregates,
   output = write_nb_aggregates,
   alignment = double
);

CREATE OR REPLACE FUNCTION add_nb_aggregates(nb_aggregates, nb_aggregates)
    RETURNS nb_aggregates
    AS :FACTML_LIBRARY, 'pg_add_nb_aggregates'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION sub_nb_aggregates(nb_aggregates, nb_aggregates)
    RETURNS nb_aggregates
    AS :FACTML_LIBRARY, 'pg_sub_nb_aggregates'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION mul_nb_aggregates(nb_aggregates, nb_aggregates)
    RETURNS nb_aggregates
    AS :FACTML_LIBRARY, 'pg_multiply_nb_aggregates'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR + (
    leftarg = nb_aggregates,
    rightarg = nb_aggregates,
    procedure = add_nb_aggregates,
    commutator = +
);

CREATE OPERATOR - (
    leftarg = nb_aggregates,
    rightarg = nb_aggregates,
    procedure = sub_nb_aggregates
);

CREATE OPERATOR * (
    leftarg = nb_aggregates,
    rightarg = nb_aggregates,
    procedure = mul_nb_aggregates,
    commutator = *
);

CREATE AGGREGATE sum (nb_aggregates) (
    sfunc = add_nb_aggregates,
    stype = nb_aggregates,
    COMBINEFUNC = add_nb_aggregates,
    PARALLEL = SAFE
);

CREATE OR REPLACE FUNCTION to_nb_aggregates(cont_vals float8[], cat_vals int4[])
    RETURNS nb_aggregates
    AS :FACTML_LIBRARY, 'lift_to_nb_aggregates'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
