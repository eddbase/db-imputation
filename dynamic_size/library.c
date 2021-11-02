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
#include <catalog/pg_type.h>
#include <math.h>
#include "utils/lsyscache.h"


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
        for (size_t i = 0; i < sum1_array_size(sz) - 1; i++)
            sscanf(str, " %lf, ", sum1_array(result) + i);
        sscanf(str, " %lf ], [ ", sum1_array(result) + sum1_array_size(sz) - 1);
        for (size_t i = 0; i < sum2_array_size(sz) - 1; i++)
            sscanf(str, " %lf, ", sum2_array(result) + i);
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
    ssize_t bufsz;
    if (triple->size == 0) {
        bufsz = snprintf(NULL, 0, "%d, (%d, [ ], [ ])", triple->size, triple->count);
    }
    else {
        bufsz = snprintf(NULL, 0, "%d, (%d, [ ", triple->size, triple->count);
        for (size_t i = 0; i < sum1_array_size(triple->size) - 1; i++)
            bufsz += snprintf(NULL, 0, "%lf, ", sum1_array(triple)[i]);
        bufsz += snprintf(NULL, 0, "%lf ], [ ", sum1_array(triple)[sum1_array_size(triple->size) - 1]);
        for (size_t i = 0; i < sum2_array_size(triple->size) - 1; i++)
            bufsz += snprintf(NULL, 0, "%lf, ", sum2_array(triple)[i]);
        bufsz += snprintf(NULL, 0, "%lf ])", sum2_array(triple)[sum2_array_size(triple->size) - 1]);
    }


    char *result = (char *) palloc0((bufsz+1) * sizeof(char));

    if (triple->size == 0) {
        sprintf(result, "%d, (%d, [ ], [ ])", triple->size, triple->count);
    }
    else {
        int out_char = sprintf(result, "%d, (%d, [ ", triple->size, triple->count);
        for (size_t i = 0; i < sum1_array_size(triple->size) - 1; i++)
            out_char += sprintf(result + out_char, "%lf, ", sum1_array(triple)[i]);
        out_char += sprintf(result + out_char, "%lf ], [ ", sum1_array(triple)[sum1_array_size(triple->size) - 1]);
        for (size_t i = 0; i < sum2_array_size(triple->size) - 1; i++)
            out_char += sprintf(result + out_char, "%lf, ", sum2_array(triple)[i]);
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
    uint32 size = pq_getmsgint64(buf);
    result = (Triple *) palloc0(sizeof(Triple) + (array_size(size)*sizeof(double)));
    SET_VARSIZE(result, sizeof(Triple) + array_size(size) * sizeof(double));
    result -> size = size;
    result -> count = pq_getmsgint64(buf);
    for (size_t i = 0; i < array_size(result -> size); i++)
        result->array[i] = pq_getmsgfloat8(buf);
    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(triple_send);

Datum
triple_send(PG_FUNCTION_ARGS)
{
    Triple    *triple = (Triple *) PG_GETARG_POINTER(0);
    StringInfoData buf;

    pq_begintypsend(&buf);
    pq_sendint64(&buf, triple->size);
    pq_sendint64(&buf, triple->count);
    for (size_t i = 0; i < array_size(triple -> size); i++)
        pq_sendfloat8(&buf, triple->array[i]);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

PG_FUNCTION_INFO_V1(cofactor_send);

Datum cofactor_send(PG_FUNCTION_ARGS)
{
    Triple *triple = (Triple *) PG_GETARG_VARLENA_P(0);
    const double *data = sum2_array(triple); // C array
    Datum *d = (Datum *) palloc(sizeof(Datum) * sum2_array_size(triple->size));
    ArrayType *a;
    for (int i = 0; i < sum2_array_size(triple->size); i++)
        d[i] = Float8GetDatum(data[i]);
    a = construct_array(d, sum2_array_size(triple->size), FLOAT8OID, sizeof(float8), true, 'd');
    PG_RETURN_ARRAYTYPE_P(a);
}

void compute_gradient(double* grad, Triple *triple, const double* learned_coeff, int label) {
    /* Compute Sigma * Theta */
    for (size_t col = 0; col < (triple->size); col++) {
        //grad[col + 1] = 0.0;
        grad[col + 1] = sum1_array(triple)[col] * learned_coeff[0];
        for (size_t row = 0; row < (triple->size); row++) {
            if (row > col)
                grad[col + 1] += learned_coeff[row + 1] *
                                 sum2_array(triple)[(col * triple->size) - (((col) * (col+1)) / 2) + row];//symmetric cofactor matrix
            else
                grad[col + 1] += learned_coeff[row + 1] *
                                 sum2_array(triple)[(row * triple->size) - (((row) * (row+1)) / 2) + col];
        }
        //todo check -1
        grad[col + 1] /= triple->count;
    }

    grad[0] = triple->count * learned_coeff[0];
    //compute grad[0] (bias)
    for (size_t col = 0; col < triple->size; col++)
        grad[0] += sum1_array(triple)[col] * learned_coeff[col+1];

    grad[label + 1] = 0.0;
}

double compute_error(Triple *triple, double lambda, const double* learned_coeff) {
    double error = 0.0;
    /* Compute 1/N * Theta^T * Sigma * Theta */
    for (size_t col = 0; col < (triple->size); col++) {
        double tmp = 0.0;
        for (size_t row = 0; row < (triple->size); row++) {
            if (row > col)
                tmp += learned_coeff[row + 1] *
                       sum2_array(triple)[(col * triple->size) - (((col) * (col+1)) / 2) + row];
            else
                tmp += learned_coeff[row + 1] *
                       sum2_array(triple)[(row * triple->size) - (((row) * (row+1)) / 2) + col];
        }
        error += learned_coeff[col] * tmp;
    }
    error /= triple->count;

    /* Add the regulariser to the error */
    double param_norm = 0.0;
    for (size_t i = 1; i < (triple->size) + 1; i++) {
        param_norm += learned_coeff[i] * learned_coeff[i];
    }
    param_norm -= 1;    // param_norm -= params[LABEL_IDX] * params[LABEL_IDX];
    error += lambda * param_norm;

    return error / 2;
}

inline double compute_step_size(double step_size, const double* params, const double* prev_params, const double* grad, const double *prev_grad, int n_params) {
    double DSS = 0.0, GSS = 0.0, DGS = 0.0;
    for (size_t i = 0; i < n_params; i++) {

        double paramDiff = params[i] - prev_params[i];
        double gradDiff = grad[i] - prev_grad[i];

        DSS += paramDiff * paramDiff;
        GSS += gradDiff * gradDiff;
        DGS += paramDiff * gradDiff;
    }

    if (DGS == 0.0 || GSS == 0.0) return step_size;

    double Ts = DSS / DGS;
    double Tm = DGS / GSS;

    if (Tm < 0.0 || Ts < 0.0) return step_size;

    return (Tm / Ts > 0.5) ? Tm : Ts - 0.5 * Tm;
}


PG_FUNCTION_INFO_V1(converge);
Datum converge(PG_FUNCTION_ARGS)
{
    Triple *triple = (Triple *) PG_GETARG_VARLENA_P(0);
    int label = PG_GETARG_INT64(1);
    double step_size = PG_GETARG_FLOAT8(2);
    double lambda = PG_GETARG_FLOAT8(3);

    size_t num_params = (triple->size)+1;
    //double *cofactor_matrix = (double *) palloc(sizeof(double) * sum2_array_size(triple->size));

    double *grad = (double *) palloc0(sizeof(double) * num_params);
    double *prev_grad = (double *) palloc0(sizeof(double) * num_params);
    double *learned_coeff = (double *) palloc(sizeof(double) * num_params);
    double *prev_learned_coeff = (double *) palloc0(sizeof(double) * num_params);

    for(size_t i = 0; i < num_params; i++)
        learned_coeff[i] = ((double) (rand() % 800 + 1) - 400) / 100;

    prev_learned_coeff[label+1] = -1;
    learned_coeff[label+1] = -1;

    //sigma[0,:] = cofactor.sum1[i];

    compute_gradient(grad, triple, learned_coeff, label);
    double gradient_norm = grad[0] * grad[0];//bias
    for (size_t i = 1; i < num_params; i++) {
        double upd = grad[i] + lambda * learned_coeff[i];
        gradient_norm += upd * upd;
    }
    gradient_norm -= lambda * lambda;   // label correction
    double first_gradient_norm = sqrt(gradient_norm);

    double prev_error = compute_error(triple, lambda, learned_coeff);

    double update[num_params];
    int num_iterations = 1;
    do {
        // Update parameters and compute gradient norm
        update[0] = grad[0];
        gradient_norm = update[0] * update[0];
        prev_learned_coeff[0] = learned_coeff[0];
        prev_grad[0] = grad[0];
        learned_coeff[0] = learned_coeff[0] - step_size * update[0];
        double dparam_norm = update[0] * update[0];
        for (size_t i = 1; i < num_iterations; i++) {
            update[i] = grad[i] + lambda * learned_coeff[i];
            gradient_norm += update[i] * update[i];
            prev_learned_coeff[i] = learned_coeff[i];
            prev_grad[i] = grad[i];
            learned_coeff[i] = learned_coeff[i] - step_size * update[i];
            dparam_norm += update[i] * update[i];
        }
        learned_coeff[label] = -1;
        gradient_norm -= lambda * lambda;   // label correction
        dparam_norm = step_size * sqrt(dparam_norm);

        double error = compute_error(triple, lambda, learned_coeff);

        /* Backtracking Line Search: Decrease step_size until condition is satisfied */
        size_t backtracking_steps = 0;
        while (error > prev_error - (step_size / 2) * gradient_norm && backtracking_steps < 500) {
            step_size /= 2;            // Update parameters based on the new step_size.

            dparam_norm = 0.0;
            for (size_t i = 0; i < num_params; i++) {
                double newp = prev_learned_coeff[i] - step_size * update[i];
                double dp = learned_coeff[i] - newp;
                learned_coeff[i] = newp;
                dparam_norm += dp * dp;
            }
            dparam_norm = sqrt(dparam_norm);

            learned_coeff[label] = -1;
            error = compute_error(triple, lambda, learned_coeff);
            backtracking_steps++;
        }

        /* Normalized residual stopping condition */
        gradient_norm = sqrt(gradient_norm);
        if (dparam_norm < 1e-20 ||
            gradient_norm / (first_gradient_norm + 0.001) < 1e-5) {
            break;
        }

        compute_gradient(grad, triple, learned_coeff, label);
        step_size = compute_step_size(step_size, learned_coeff, prev_learned_coeff, grad, prev_grad, num_params);
        prev_error = error;
        num_iterations++;
    } while (num_iterations < 1000);

    //export params in pgpsql
    const double *data = learned_coeff; // C array
    Datum *d = (Datum *) palloc(sizeof(Datum) * num_params);
    ArrayType *a;
    for (int i = 0; i < num_params; i++)
        d[i] = Float8GetDatum(data[i]);
    a = construct_array(d, sum2_array_size(triple->size), FLOAT8OID, sizeof(float8), true, 'd');
    PG_RETURN_ARRAYTYPE_P(a);
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

PG_FUNCTION_INFO_V1(triple_sub);

Datum
triple_sub(PG_FUNCTION_ARGS)
{
    Triple    *triple1 = (Triple *) PG_GETARG_POINTER(0);
    Triple    *triple2 = (Triple *) PG_GETARG_POINTER(1);
    uint32 array_sz = array_size(triple1->size);
    Triple *result = (Triple *) palloc0(sizeof(Triple) + array_sz * sizeof(double));
    SET_VARSIZE(result, sizeof(Triple) + array_sz * sizeof(double));
    result->size = triple1->size;
    result->count = triple1->count - triple2->count;
    for (size_t i = 0; i < array_sz; i++) {
        result->array[i] = triple1->array[i] - triple2->array[i];
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

PG_FUNCTION_INFO_V1(partial_mul);

Datum partial_mul(PG_FUNCTION_ARGS)
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

PG_FUNCTION_INFO_V1(update_triple);

Datum update_triple(PG_FUNCTION_ARGS) {
    Oid arrayElementType1;
    int16 arrayElementTypeWidth1;
    bool arrayElementTypeByValue1;
    char arrayElementTypeAlignmentCode1;
    Datum *arrayContent1;
    bool *arrayNullFlags1;
    int arrayLength1;

    ArrayType *array1 = PG_GETARG_ARRAYTYPE_P(0);
    Triple *triple = (Triple *) PG_GETARG_VARLENA_P(1);
    int col = PG_GETARG_INT64(2);

    Triple *result = (Triple *) palloc0(sizeof(Triple) + (triple->size * sizeof(double)));
    SET_VARSIZE(result, sizeof(Triple) + (triple->size * sizeof(double)));

    result->size = triple->size;
    result->count = triple->count;
    for (size_t i=0; array_size(triple->size); i++)
    {
        result->array[i] = triple->array[i];
    }

    arrayElementType1 = ARR_ELEMTYPE(array1);
    get_typlenbyvalalign(arrayElementType1, &arrayElementTypeWidth1, &arrayElementTypeByValue1, &arrayElementTypeAlignmentCode1);
    deconstruct_array(array1, arrayElementType1, arrayElementTypeWidth1, arrayElementTypeByValue1, arrayElementTypeAlignmentCode1, &arrayContent1, &arrayNullFlags1, &arrayLength1);

    for (size_t i = 0; i < arrayLength1; i++)
    {
        if (i > col)
            sum2_array(result)[(col * triple->size) - (((col) * (col+1)) / 2) + i] += DatumGetFloat8(arrayContent1[i]);
        else
            sum2_array(result)[(i * triple->size) - (((i) * (i+1)) / 2) + col] += DatumGetFloat8(arrayContent1[i]);
    }
    PG_RETURN_POINTER(result);
}