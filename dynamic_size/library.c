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
    int32 vl_len_;		/* varlena header (do not touch directly!) */
    uint32 size;
    int	count;
    double array[FLEXIBLE_ARRAY_MEMBER];
}
Triple;

inline uint32 array_size(uint32 sz) { return sz * (sz + 3) / 2; }
inline uint32 sum1_array_size(uint32 sz) { return sz; }
inline uint32 sum2_array_size(uint32 sz) { return sz * (sz + 1) / 2; }

inline double * sum1_array(Triple *t) { return t->array; }
inline double * sum2_array(Triple *t) { return t->array + t->size; }

/*****************************************************************************
 * Input/Output functions
 *****************************************************************************/

PG_FUNCTION_INFO_V1(triple_in);

Datum triple_in(PG_FUNCTION_ARGS)
{
    char *str = PG_GETARG_CSTRING(0);

    uint32 sz;
    sscanf(str, "%d, ", &sz);

    uint32 array_sz = array_size(sz);
    Triple *result = (Triple *) palloc0(sizeof(Triple) + array_sz * sizeof(double));
    SET_VARSIZE(result, sizeof(Triple) + array_sz * sizeof(double));

    result -> size = sz;

    if (sz > 0) {
        sscanf(str, "(%d, [ ", &(result->count));

        for (size_t i = 0; i < sum1_array_size(sz) - 1; i++) {
            sscanf(str, " %lf, ", sum1_array(result) + i);
        }
        sscanf(str, " %lf ], [ ", sum1_array(result) + sum1_array_size(sz) - 1);

        for (size_t i = 0; i < sum2_array_size(sz) - 1; i++) {
            sscanf(str, " %lf, ", sum2_array(result) + i);
        }
        sscanf(str, " %lf ])", sum2_array(result) + sum2_array_size(sz) - 1);
    }
    else {
        sscanf(str, "(%d, [ ], [ ])", &(result->count));
    }

    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(triple_out);

Datum triple_out(PG_FUNCTION_ARGS)
{
    Triple *triple = (Triple *) PG_GETARG_VARLENA_P(0);
    
    // TODO: fix buffer size
    size_t length_out = 255 * sizeof(char);
    char *result = (char *) palloc0(length_out);

    if (triple->size == 0) {
        sprintf(result, "%d, (%d, [ ], [ ])", triple->size, triple->count);
    }
    else {
        int out_char = sprintf(result, "%d, (%d, [ ", triple->size, triple->count);

        for (size_t i = 0; i < sum1_array_size(triple->size) - 1; i++) {
            out_char += sprintf(result + out_char, "%lf, ", sum1_array(triple)[i]);
        }
        out_char += sprintf(result + out_char, "%lf ], [ ", sum1_array(triple)[sum1_array_size(triple->size) - 1]);

        for (size_t i = 0; i < sum2_array_size(triple->size) - 1; i++) {
            out_char += sprintf(result + out_char, "%lf, ", sum2_array(triple)[i]);
        }
        out_char += sprintf(result + out_char, "%lf ])", sum2_array(triple)[sum2_array_size(triple->size) - 1]);
    }

    PG_RETURN_CSTRING(result);
}

//binary in/out

PG_FUNCTION_INFO_V1(triple_recv);

Datum
triple_recv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
    Triple *result;

    result = (Triple *) palloc(sizeof(Triple));
    result->count = pq_getmsgfloat8(buf);
    // TODO: finish this

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
    // TODO: finish this

    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}


PG_FUNCTION_INFO_V1(lift_const);

Datum lift_const(PG_FUNCTION_ARGS) {
    int a = (int) PG_GETARG_INT64(0);

    Triple *result = (Triple *) palloc0(sizeof(Triple));
    SET_VARSIZE(result, sizeof(Triple));

    result->size = 0;
    result->count = a;
    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(lift);

Datum lift(PG_FUNCTION_ARGS) {
    double a = (double) PG_GETARG_FLOAT8(0);

    Triple *result = (Triple *) palloc0(sizeof(Triple) + 2 * sizeof(double));
    SET_VARSIZE(result, sizeof(Triple) + 2 * sizeof(double));

    result->size = 1;
    result->count = 1;
    result->array[0] = a;
    result->array[1] = a * a;
    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(lift2);

Datum lift2(PG_FUNCTION_ARGS) {
    double a = (double) PG_GETARG_FLOAT8(0);
    double b = (double) PG_GETARG_FLOAT8(1);

    Triple *result = (Triple *) palloc0(sizeof(Triple) + 5 * sizeof(double));
    SET_VARSIZE(result, sizeof(Triple) + 5 * sizeof(double));

    result->size = 2;
    result->count = 1;
    result->array[0] = a;
    result->array[1] = b;
    result->array[2] = a * a;
    result->array[3] = a * b;
    result->array[4] = b * b;
    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(triple_add);

Datum triple_add(PG_FUNCTION_ARGS)
{
    Triple *a = (Triple *) PG_GETARG_VARLENA_P(0);
    Triple *b = (Triple *) PG_GETARG_VARLENA_P(1);

    uint32 array_sz = array_size(a->size);
    Triple *result = (Triple *) palloc0(sizeof(Triple) + array_sz * sizeof(double));
    SET_VARSIZE(result, sizeof(Triple) + array_sz * sizeof(double));

    result->size = a->size;
    result->count = a->count + b->count;
    for (size_t i = 0; i < array_sz; i++) {
        result->array[i] = a->array[i] + b->array[i];
    }
    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(triple_mul);

Datum triple_mul(PG_FUNCTION_ARGS)
{
    Triple *a = (Triple *) PG_GETARG_VARLENA_P(0);
    Triple *b = (Triple *) PG_GETARG_VARLENA_P(1);

    uint32 array_sz = array_size(a->size + b->size);
    Triple *result = (Triple *) palloc0(sizeof(Triple) + array_sz * sizeof(double));
    SET_VARSIZE(result, sizeof(Triple) + array_sz * sizeof(double));

    result->size = a->size + b->size;
    result->count = a->count * b->count;
    
    for (size_t i = 0; i < a->size; i++) {
        sum1_array(result)[i] = sum1_array(a)[i] * b->count;
    }
    for (size_t i = 0; i < b->size; i++) {
        sum1_array(result)[i + a->size] = sum1_array(b)[i] * a->count;
    }

    double *out = sum2_array(result);
    double *in_a = sum2_array(a);
    for (size_t i = 0; i < a->size; i++) {
        for (size_t j = i; j < a->size; j++) {
            *out++ = (*in_a++) * b->count;
        }        
        for (size_t j = 0; j < b->size; j++) {
            *out++ = sum1_array(a)[i] * sum1_array(b)[j];
        }
    }
    double *in_b = sum2_array(b);
    for (size_t i = 0; i < sum2_array_size(b->size); i++) {
        *out++ = (*in_b++) * a->count;
    }

    PG_RETURN_POINTER(result);
}
