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
    //char vl_len_[4];
    int32		vl_len_;		/* varlena header (do not touch directly!) */
    //int32		padding;
    int		count;
    int size;
    double array[FLEXIBLE_ARRAY_MEMBER];//1
}
Triple;


/*****************************************************************************
 * Input/Output functions
 *****************************************************************************/

inline double* sum_vector(double *array, int size) { return array; }

inline double* matrix(double *array, int size) { return &array[size]; }

PG_FUNCTION_INFO_V1(triple_in);

Datum triple_in(PG_FUNCTION_ARGS)
{
    char	*str = PG_GETARG_CSTRING(0);
    int		x;
    int attrs;

    sscanf(str, "%d , ", &attrs);

    //Triple *result = (Triple *) palloc(VARHDRSZ + (sizeof(int)*2) + (2*sizeof(double *)) + ((((attrs*(attrs+1))/2) + attrs) * sizeof(double)));
    //SET_VARSIZE(result, VARHDRSZ + (sizeof(int)*2) + (2*sizeof(double *)) + ((((attrs*(attrs+1))/2) + attrs) * sizeof(double)));

    Triple *result = (Triple *) palloc0(sizeof(Triple) + ((((attrs*(attrs+1))/2) + attrs) * sizeof(double)));
    SET_VARSIZE(result, sizeof(Triple) + ((((attrs*(attrs+1))/2) + attrs) * sizeof(double)));

    result -> size = attrs;

    sscanf(str, " ( %d , [ ", &x);

    for (int i=0;i<attrs-1;i++)
        sscanf(str, " %lf , ", &(result->array[i]));

    sscanf(str, " %lf ] , [", &(result->array[attrs-1]));

    for (int i=attrs; i<attrs+(attrs*attrs) -1; i++)
        sscanf(str, " %lf , ", &(result->array[i]));

    sscanf(str, " %lf] ", &(result->array[attrs+(attrs*attrs)-1]));

    result -> count = x;
    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(lift);

Datum lift(PG_FUNCTION_ARGS) {
    double a = (double) PG_GETARG_FLOAT8(0);
    int b = (int) PG_GETARG_INT64(1);
    int attrs = (int) PG_GETARG_INT64(2);

    Triple *result = (Triple *) palloc0(sizeof(Triple) + ((((attrs*(attrs+1))/2) + attrs) * sizeof(double)));
    SET_VARSIZE(result, sizeof(Triple) + ((((attrs*(attrs+1))/2) + attrs) * sizeof(double)));
    //SET_VARSIZE(result, VARHDRSZ + (sizeof(int)*2) + (2*sizeof(double *)) + ((((attrs*(attrs+1))/2) + attrs) * sizeof(double)));
    result -> size = attrs;
    result->count = 1;
    sum_vector(result->array, result->size)[b] = a;
    if (b == 0)
        matrix(result->array, result->size)[(b*attrs)] = (a*a);
    else
        matrix(result->array, result->size)[(b*attrs) - (((b-1)*(b))/2)] = (a*a);
    PG_RETURN_POINTER(result);
}


PG_FUNCTION_INFO_V1(triple_out);

Datum triple_out(PG_FUNCTION_ARGS)
{
    Triple    *triple = (Triple *) PG_GETARG_VARLENA_P(0);
    size_t length_out = 255*sizeof(char);
    char	   *result = (char *) palloc0(length_out);
    int out_char = sprintf(result, "%d , (%d,[", triple->size, triple->count);
    for (int i=0;i<triple->size-1;i++)
        out_char+=sprintf(&result[out_char], "%lf,", sum_vector(triple->array, triple->size)[i]);
    out_char+=sprintf(&result[out_char], "%lf],[", sum_vector(triple->array, triple->size)[triple->size-1]);

    for (int i=0;i<(triple->size*(triple->size+1)/2)-1;i++)
        out_char+=sprintf(&result[out_char], "%lf,", matrix(triple->array, triple->size)[i]);
    out_char+=sprintf(&result[out_char], "%lf])", matrix(triple->array, triple->size)[(triple->size*(triple->size+1)/2)-1]);

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
    //todo

    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(triple_send);

Datum
triple_send(PG_FUNCTION_ARGS)
{
    Triple    *triple = (Triple *) PG_GETARG_POINTER(0);
    StringInfoData buf;

    pq_begintypsend(&buf);
    pq_sendfloat8(&buf, triple->count);
    //todo

    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}


PG_FUNCTION_INFO_V1(triple_add);

Datum triple_add(PG_FUNCTION_ARGS)
{
    Triple    *a = (Triple *) PG_GETARG_VARLENA_P(0);
    Triple    *b = (Triple *) PG_GETARG_VARLENA_P(1);

    Triple *result = (Triple *) palloc0(sizeof(Triple) + ((((a->size*(a->size+1))/2) + a->size) * sizeof(double)));
    SET_VARSIZE(result, sizeof(Triple) + ((((a->size*(a->size+1))/2) + a->size) * sizeof(double)));

    result -> size = a -> size;
    result->count = a->count + b->count;

    for(unsigned int i=0;i<result->size;i++) {
        //elog(WARNING,"sum vector init: %lf", sum_vector(result->array, result->size)[i]);
        sum_vector(result->array, result->size)[i] = sum_vector(a->array, a->size)[i] + sum_vector(b->array, b->size)[i];
        //elog(WARNING,"a: %lf + b: %lf = %lf (%lf stored)", sum_vector(a->array, a->size)[i], sum_vector(b->array, b->size)[i], sum_vector(a->array, a->size)[i] + sum_vector(b->array, b->size)[i], sum_vector(result->array, result->size)[i]);
    }

    for(unsigned int i=0;i<(result->size*(result->size+1))/2;i++)
        matrix(result->array, result->size)[i] = matrix(a->array, a->size)[i] + matrix(b->array, b->size)[i];

    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(triple_mul);

Datum triple_mul(PG_FUNCTION_ARGS)
{
    Triple    *a = (Triple *) PG_GETARG_VARLENA_P(0);
    Triple    *b = (Triple *) PG_GETARG_VARLENA_P(1);

    //Triple *result = (Triple *) palloc0(VARHDRSZ + (sizeof(int)*2) + (2*sizeof(double *)) + ((((a->size*(a->size+1))/2) + a->size) * sizeof(double)));
    //SET_VARSIZE(result, VARHDRSZ + (sizeof(int)*2) + (2*sizeof(double *)) + ((((a->size*(a->size+1))/2) + a->size) * sizeof(double)));

    Triple *result = (Triple *) palloc0(sizeof(Triple) + ((((a->size*(a->size+1))/2) + a->size) * sizeof(double)));
    SET_VARSIZE(result, sizeof(Triple) + ((((a->size*(a->size+1))/2) + a->size) * sizeof(double)));

    result->count = a->count * b->count;
    result -> size = a -> size;

    for(int i=0;i<a->size;i++)
        sum_vector(result->array, result->size)[i] = (a->count * sum_vector(b->array, b->size)[i]) + (b->count * sum_vector(a->array, a->size)[i]);

    for(int i=0;i<(a->size*(a->size+1))/2;i++)
        matrix(result->array, result->size)[i] = (b->count * matrix(a->array, a->size)[i]) + (a->count * matrix(b->array, b->size)[i]);

    for (int i=0;i<a->size;i++) {
        for (int j = 0; j < a->size; j++) {
            matrix(result->array, result->size)[(i * a->size) + j] += (sum_vector(a->array, a->size)[i] * sum_vector(b->array, b->size)[j]);
            matrix(result->array, result->size)[(i * a->size) + j] += (sum_vector(b->array, b->size)[i] * sum_vector(a->array, a->size)[j]);
        }
    }

    PG_RETURN_POINTER(result);
}
