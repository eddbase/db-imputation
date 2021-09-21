/*
 * src/tutorial/complex.c
 *
 ******************************************************************************
  This file contains routines that can be bound to a Postgres backend and
  called by the backend in the process of processing queries.  The calling
  format for these routines is dictated by Postgres architecture.
******************************************************************************/

#include "postgres.h"

#include "fmgr.h"
#include "libpq/pqformat.h"		/* needed for send/recv functions */

PG_MODULE_MAGIC;

typedef struct Triple
{
    double		count;
    double      item;
    //double*		sum_vector;
    //double*     cofactor_matrix;
    //int size;
}
Triple;


/*****************************************************************************
 * Input/Output functions
 *****************************************************************************/

PG_FUNCTION_INFO_V1(triple_in);

Datum triple_in(PG_FUNCTION_ARGS)
{
    char	   *str = PG_GETARG_CSTRING(0);
    double		x,
            y;
    Triple    *result;

    if (sscanf(str, " ( %lf , %lf )", &x, &y) != 2)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        errmsg("invalid input syntax for type %s: \"%s\"", "", str)));

    result = (Triple *) palloc(sizeof(Triple));
    result->count = x;
    result->item = y;
    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(triple_out);

Datum triple_out(PG_FUNCTION_ARGS)
{
    Triple    *triple = (Triple *) PG_GETARG_POINTER(0);
    char	   *result;

    result = psprintf("(%g,%g)", triple->count, triple->item);
    PG_RETURN_CSTRING(result);
}

//binary in/out

PG_FUNCTION_INFO_V1(triple_recv);

Datum
triple_recv(PG_FUNCTION_ARGS)
{
    StringInfo  buf = (StringInfo) PG_GETARG_POINTER(0);
    Triple    *result;

    result = (Triple *) palloc(sizeof(Triple));
    result->count = pq_getmsgfloat8(buf);
    result->item = pq_getmsgfloat8(buf);
    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(triple_send);

Datum
triple_send(PG_FUNCTION_ARGS)
{
    Triple    *complex = (Triple *) PG_GETARG_POINTER(0);
    StringInfoData buf;

    pq_begintypsend(&buf);
    pq_sendfloat8(&buf, complex->count);
    pq_sendfloat8(&buf, complex->item);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

//+ operator

PG_FUNCTION_INFO_V1(triple_add);

Datum triple_add(PG_FUNCTION_ARGS)
{
    Triple    *a = (Triple *) PG_GETARG_POINTER(0);
    Triple    *b = (Triple *) PG_GETARG_POINTER(1);
    Triple    *result;

    result = (Triple *) palloc(sizeof(Triple));
    result->count = a->count + b->count;
    result->item = a->item + b->item;

    /*
    for(int i=0;i<a->size;i++)
        result->sum_vector[i] = a->sum_vector[i] + b->sum_vector[i];

    for(int i=0;i<(a->size * a->size);i++)
        result->cofactor_matrix[i] = a->cofactor_matrix[i] + b->cofactor_matrix[i];
    */
    PG_RETURN_POINTER(result);
}

/*
PG_FUNCTION_INFO_V1(triple_mul);

Datum triple_mul(PG_FUNCTION_ARGS)
{
    Triple    *a = (Triple *) PG_GETARG_POINTER(0);
    Triple    *b = (Triple *) PG_GETARG_POINTER(1);
    Triple    *result;

    result = (Triple *) palloc(sizeof(Triple));
    result->count = a->count * b->count;
    for(int i=0;i<a->size;i++)
        result->sum_vector[i] = (a->count * a->sum_vector[i]) + (b->count * b->sum_vector[i]);

    for(int i=0;i<(a->size * a->size);i++)
        result->cofactor_matrix[i] = a->cofactor_matrix[i] + b->cofactor_matrix[i];

    PG_RETURN_POINTER(result);
}

*/

/*****************************************************************************
 * Operator class for defining B-tree index
 *
 * It's essential that the comparison operators and support function for a
 * B-tree index opclass always agree on the relative ordering of any two
 * data values.  Experience has shown that it's depressingly easy to write
 * unintentionally inconsistent functions.  One way to reduce the odds of
 * making a mistake is to make all the functions simple wrappers around
 * an internal three-way-comparison function, as we do here.
 *****************************************************************************/

/*
#define Mag(c)	((c)->x*(c)->x + (c)->y*(c)->y)

static int
complex_abs_cmp_internal(Complex * a, Complex * b)
{
    double		amag = Mag(a),
            bmag = Mag(b);

    if (amag < bmag)
        return -1;
    if (amag > bmag)
        return 1;
    return 0;
}


PG_FUNCTION_INFO_V1(complex_abs_lt);

Datum
complex_abs_lt(PG_FUNCTION_ARGS)
{
    Complex    *a = (Complex *) PG_GETARG_POINTER(0);
    Complex    *b = (Complex *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(complex_abs_cmp_internal(a, b) < 0);
}

PG_FUNCTION_INFO_V1(complex_abs_le);

Datum
complex_abs_le(PG_FUNCTION_ARGS)
{
    Complex    *a = (Complex *) PG_GETARG_POINTER(0);
    Complex    *b = (Complex *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(complex_abs_cmp_internal(a, b) <= 0);
}

PG_FUNCTION_INFO_V1(complex_abs_eq);

Datum
complex_abs_eq(PG_FUNCTION_ARGS)
{
    Complex    *a = (Complex *) PG_GETARG_POINTER(0);
    Complex    *b = (Complex *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(complex_abs_cmp_internal(a, b) == 0);
}

PG_FUNCTION_INFO_V1(complex_abs_ge);

Datum
complex_abs_ge(PG_FUNCTION_ARGS)
{
    Complex    *a = (Complex *) PG_GETARG_POINTER(0);
    Complex    *b = (Complex *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(complex_abs_cmp_internal(a, b) >= 0);
}

PG_FUNCTION_INFO_V1(complex_abs_gt);

Datum
complex_abs_gt(PG_FUNCTION_ARGS)
{
    Complex    *a = (Complex *) PG_GETARG_POINTER(0);
    Complex    *b = (Complex *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(complex_abs_cmp_internal(a, b) > 0);
}

PG_FUNCTION_INFO_V1(complex_abs_cmp);

Datum
complex_abs_cmp(PG_FUNCTION_ARGS)
{
    Complex    *a = (Complex *) PG_GETARG_POINTER(0);
    Complex    *b = (Complex *) PG_GETARG_POINTER(1);

    PG_RETURN_INT32(complex_abs_cmp_internal(a, b));
}

*/

/*
 * CREATE FUNCTION complex_add(complex, complex)
    RETURNS complex
    AS 'filename', 'complex_add'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR + (
    leftarg = complex,
    rightarg = complex,
    procedure = complex_add,
    commutator = +
);

 SELECT (a + b) AS c FROM test_complex;

        c

 */