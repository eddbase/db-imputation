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

#define SIZE 2
typedef struct Triple
{
    double		count;
    //double      item;
    double		sum_vector[SIZE];
    double		matrix[SIZE*SIZE];
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
    double		x;
    Triple    *result;
    result = (Triple *) palloc(sizeof(Triple));
    sscanf(str, " ( %lf , [ %lf , %lf ] , [ %lf , %lf , %lf , %lf ] )", &x, &(result->sum_vector[0]), &(result->sum_vector[1]) , &(result->matrix[0]) , &(result->matrix[1]) , &(result->matrix[2]) , &(result->matrix[3]));
    //result -> sum_vector = (double *) palloc(sizeof(double)*SIZE);

    //if (sscanf(str, "(%lf,[%lf,%lf])", &x, &(result->sum_vector[0]), &(result->sum_vector[1])) != 3) {
        //ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid input syntax for type %s: \"%s\"", "", str)));
    //}

    result->count = x;
    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(triple_out);

Datum triple_out(PG_FUNCTION_ARGS)
{
    Triple    *triple = (Triple *) PG_GETARG_POINTER(0);
    char	   *result;

    result = psprintf("(%g,[%g,%g],[%g,%g,%g,%g])", triple->count, triple -> sum_vector[0], triple -> sum_vector[1], triple -> matrix[0], triple -> matrix[1], triple -> matrix[2], triple -> matrix[3]);
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

    for(unsigned int i=0;i<SIZE;i++){
        result->sum_vector[i] = pq_getmsgfloat8(buf);
    }

    for(unsigned int i=0;i<SIZE*SIZE;i++){
        result->matrix[i] = pq_getmsgfloat8(buf);
    }

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

    for(unsigned int i=0;i<SIZE;i++){
        pq_sendfloat8(&buf, triple->sum_vector[i]);
    }

    for(unsigned int i=0;i<SIZE*SIZE;i++){
        pq_sendfloat8(&buf, triple->matrix[i]);
    }

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

    for(unsigned int i=0;i<SIZE;i++)
        result->sum_vector[i] = a->sum_vector[i] + b->sum_vector[i];


    for(unsigned int i=0;i<SIZE*SIZE;i++)
        result->matrix[i] = a->matrix[i] + b->matrix[i];

    PG_RETURN_POINTER(result);
}


PG_FUNCTION_INFO_V1(triple_mul);

Datum triple_mul(PG_FUNCTION_ARGS)
{
    Triple    *a = (Triple *) PG_GETARG_POINTER(0);
    Triple    *b = (Triple *) PG_GETARG_POINTER(1);
    Triple    *result;

    result = (Triple *) palloc(sizeof(Triple));
    result->count = a->count * b->count;
    for(int i=0;i<SIZE;i++)
        result->sum_vector[i] = (a->count * b->sum_vector[i]) + (b->count * a->sum_vector[i]);

    for(int i=0;i<SIZE*SIZE;i++)
        result->matrix[i] = (b->count * a->matrix[i]) + (a->count * b->matrix[i]);

    for (int i=0;i<SIZE;i++) {
        for (int j = 0; j < SIZE; j++) {
            result->matrix[(i * SIZE) + j] += (a->sum_vector[i] * b->sum_vector[j]);
            result->matrix[(i * SIZE) + j] += (b->sum_vector[i] * a->sum_vector[j]);
        }
    }

    PG_RETURN_POINTER(result);
}
