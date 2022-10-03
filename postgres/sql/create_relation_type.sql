--SET client_min_messages TO WARNING;

LOAD :FACTML_LIBRARY;

DROP TYPE relation CASCADE;
CREATE TYPE relation;

CREATE OR REPLACE FUNCTION read_relation(cstring)
    RETURNS relation
    AS :FACTML_LIBRARY, 'read_relation'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION write_relation(relation)
    RETURNS cstring
    AS :FACTML_LIBRARY, 'write_relation'
    LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE relation (
   internallength = VARIABLE,
   input = read_relation,
   output = write_relation,
   alignment = double
);

CREATE OR REPLACE FUNCTION add_relations(relation, relation)
    RETURNS relation
    AS :FACTML_LIBRARY, 'pg_add_relations'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION sub_relations(relation, relation)
    RETURNS relation
    AS :FACTML_LIBRARY, 'pg_subtract_relations'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION mul_relations(relation, relation)
    RETURNS relation
    AS :FACTML_LIBRARY, 'pg_multiply_relations'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR + (
    leftarg = relation,
    rightarg = relation,
    procedure = add_relations,
    commutator = +
);

CREATE OPERATOR - (
    leftarg = relation,
    rightarg = relation,
    procedure = sub_relations
);

CREATE OPERATOR * (
    leftarg = relation,
    rightarg = relation,
    procedure = mul_relations,
    commutator = *
);

CREATE AGGREGATE sum (relation) (
    sfunc = add_relations,
    stype = relation,
    COMBINEFUNC = add_relations,
    PARALLEL = SAFE
);

CREATE OR REPLACE FUNCTION to_relation(i int4)
    RETURNS relation
    AS :FACTML_LIBRARY, 'lift_to_relation'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
