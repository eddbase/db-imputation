\set FACTMICE_LIBRARY '''/Users/massimo/Documents/imputation/custom_datatype/cmake-build-debug/libfactMICE.dylib'''

LOAD :FACTMICE_LIBRARY;
DROP TYPE triple CASCADE;

CREATE TYPE triple;

CREATE OR REPLACE FUNCTION triple_in(cstring)
    RETURNS triple
    AS :FACTMICE_LIBRARY 
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION triple_out(triple)
    RETURNS cstring
    AS :FACTMICE_LIBRARY
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION triple_recv(internal)
   RETURNS triple
   AS :FACTMICE_LIBRARY
   LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION triple_send(triple)
   RETURNS bytea
   AS :FACTMICE_LIBRARY
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
    AS :FACTMICE_LIBRARY, 'triple_add'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION triple_sub(triple, triple)
    RETURNS triple
    AS :FACTMICE_LIBRARY, 'triple_sub'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION triple_mul(triple, triple)
    RETURNS triple
    AS :FACTMICE_LIBRARY, 'triple_mul'
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
    RETURNS float8[] 
    AS :FACTMICE_LIBRARY, 'cofactor_send'
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
    AS :FACTMICE_LIBRARY, 'lift_const'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION lift(i float)
    RETURNS triple
    AS :FACTMICE_LIBRARY, 'lift'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION lift2(i float, j float)
    RETURNS triple
    AS :FACTMICE_LIBRARY, 'lift2'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION lift3(i float, j float, k float)
    RETURNS triple
    AS :FACTMICE_LIBRARY, 'lift3'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION lift4(i float, j float, k float, l float)
    RETURNS triple
    AS :FACTMICE_LIBRARY, 'lift4'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION lift5(i float, j float, k float, l float, m float)
    RETURNS triple
    AS :FACTMICE_LIBRARY, 'lift5'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION converge(triple triple, label_idx int, step_size float8, lambda float8, max_iterations int)
    RETURNS float8[]
    AS :FACTMICE_LIBRARY, 'converge'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION update_triple(arr_updates float8[], triple triple, col_update int)
    RETURNS triple
    AS :FACTMICE_LIBRARY, 'converge'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
