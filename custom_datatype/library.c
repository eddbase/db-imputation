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

void build_sigma_matrix(Triple *triple, double *sigma, size_t param_size) {
    sigma[0] = triple->count;

    for (size_t i = 0; i < (param_size-1); i++) {
        sigma[i + 1] = sum1_array(triple)[i];
        sigma[(i + 1) * param_size] = sum1_array(triple)[i];
    }

    for (size_t row = 0; row < (param_size-1); row++) {
        for (size_t col = 0; col < (param_size-1); col++) {
            if (row > col)
                sigma[((row+1) * param_size) + (col+1)] = sum2_array(triple)[(col * triple->size) - (((col) * (col+1)) / 2) + row];
            else
                sigma[((row+1) * param_size) + (col+1)] = sum2_array(triple)[(row * triple->size) - (((row) * (row+1)) / 2) + col];
        }
    }
}

void compute_gradient(size_t num_params, int label_idx, double *grad, const double *sigma, const double *params) {
    /* Compute Sigma * Theta */
    for (size_t i = 0; i < num_params; i++) {
        grad[i] = 0.0;
        for (size_t j = 0; j < num_params; j++) {
            grad[i] += sigma[(i*num_params)+j] * params[j];
        }
        grad[i] /= sigma[0];//count
    }
    grad[label_idx] = 0.0;
}

double compute_error(size_t num_params, const double* sigma, const double *params, const double lambda) {
    double error = 0.0;

    /* Compute 1/N * Theta^T * Sigma * Theta */
    for (size_t i = 0; i < num_params; i++) {
        double tmp = 0.0;
        for (size_t j = 0; j < num_params; j++) {
            tmp += sigma[(i*num_params) + j] * params[j];
        }
        error += params[i] * tmp;
    }
    //elog(WARNING, "division");
    error /= sigma[0];//count

    /* Add the regulariser to the error */
    double param_norm = 0.0;
    for (size_t i = 1; i < num_params; i++) {
        param_norm += params[i] * params[i];
    }
    //elog(WARNING, "second loop");
    param_norm -= 1;    // param_norm -= params[LABEL_IDX] * params[LABEL_IDX];
    error += lambda * param_norm;

    return error / 2;
}

inline double compute_step_size(double step_size, int num_params, const double *params, const double *prev_params, const double *grad, const double *prev_grad) {
    double DSS = 0.0, GSS = 0.0, DGS = 0.0;

    for (int i = 0; i < num_params; i++) {
        //elog(WARNING, "%d ", i);
        double paramDiff = params[i] - prev_params[i];
        double gradDiff = grad[i] - prev_grad[i];

        DSS += paramDiff * paramDiff;
        GSS += gradDiff * gradDiff;
        DGS += paramDiff * gradDiff;
    }

    if (DGS == 0.0 || GSS == 0.0) return step_size;

    //elog(WARNING, "%lf %lf ", DGS, GSS);

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
    int max_num_iterations = PG_GETARG_INT64(4);

    size_t num_params = (triple->size)+1;

    double *grad = (double *) palloc0(sizeof(double) * num_params);
    double *prev_grad = (double *) palloc0(sizeof(double) * num_params);
    double *learned_coeff = (double *) palloc(sizeof(double) * num_params);
    double *prev_learned_coeff = (double *) palloc0(sizeof(double) * num_params);
    double *sigma = (double *) palloc0(sizeof(double) * num_params * num_params);
    double *update = (double *) palloc0(sizeof(double) * num_params);

    build_sigma_matrix(triple, sigma, num_params);
    for(size_t i = 0; i < num_params; i++)
        learned_coeff[i] = 0;//((double) (rand() % 800 + 1) - 400) / 100;

    prev_learned_coeff[label+1] = -1;
    learned_coeff[label+1] = -1;
    //elog(WARNING, "compute gradient");
    //compute_gradient(grad, triple, learned_coeff, label);
    compute_gradient(num_params, label+1, grad, sigma, learned_coeff);
    double gradient_norm = grad[0] * grad[0];//bias
    for (size_t i = 1; i < num_params; i++) {
        double upd = grad[i] + lambda * learned_coeff[i];
        gradient_norm += upd * upd;
    }
    gradient_norm -= lambda * lambda;   // label correction
    double first_gradient_norm = sqrt(gradient_norm);
    //elog(WARNING, "compute error");
    double prev_error = compute_error(num_params, sigma, learned_coeff, lambda);
    //double prev_error = compute_error(triple, lambda, learned_coeff);
    //size_t num_params, double* sigma, double *params, double lambda

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
        learned_coeff[label+1] = -1;
        gradient_norm -= lambda * lambda;   // label correction
        dparam_norm = step_size * sqrt(dparam_norm);
        //elog(WARNING, "compute error %d", label+1);
        double error = compute_error(num_params, sigma, learned_coeff, lambda);

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

            learned_coeff[label+1] = -1;
            //elog(WARNING, "compute again error %d", label+1);
            error = compute_error(num_params, sigma, learned_coeff, lambda);
            backtracking_steps++;
        }

        /* Normalized residual stopping condition */
        gradient_norm = sqrt(gradient_norm);
        if (dparam_norm < 1e-20 ||
            gradient_norm / (first_gradient_norm + 0.001) < 1e-5) {
            break;
        }
        //elog(WARNING, "compute gradient");
        compute_gradient(num_params, label+1, grad, sigma, learned_coeff);
        //elog(WARNING, "compute step size");
        step_size = compute_step_size(step_size, num_params, learned_coeff, prev_learned_coeff, grad, prev_grad);
        //elog(WARNING, "computed");
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

PG_FUNCTION_INFO_V1(lift3);

Datum lift3(PG_FUNCTION_ARGS) {
    double a = (double) PG_GETARG_FLOAT8(0);
    double b = (double) PG_GETARG_FLOAT8(1);
    double c = (double) PG_GETARG_FLOAT8(2);

    Triple *result = (Triple *) palloc0(sizeof(Triple) + 9 * sizeof(double));
    SET_VARSIZE(result, sizeof(Triple) + 9 * sizeof(double));

    result->size = 3;
    result->count = 1;
    result->array[0] = a;
    result->array[1] = b;
    result->array[2] = c;
    result->array[3] = a * a;
    result->array[4] = a * b;
    result->array[5] = a * c;
    result->array[6] = b * b;
    result->array[7] = b * c;
    result->array[8] = c * c;
    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(lift4);

Datum lift4(PG_FUNCTION_ARGS) {
    double a = (double) PG_GETARG_FLOAT8(0);
    double b = (double) PG_GETARG_FLOAT8(1);
    double c = (double) PG_GETARG_FLOAT8(2);
    double d = (double) PG_GETARG_FLOAT8(3);

    Triple *result = (Triple *) palloc0(sizeof(Triple) + 14 * sizeof(double));
    SET_VARSIZE(result, sizeof(Triple) + 14 * sizeof(double));

    result->size = 4;
    result->count = 1;
    result->array[0] = a;
    result->array[1] = b;
    result->array[2] = c;
    result->array[3] = d;
    result->array[4] = a * a;
    result->array[5] = a * b;
    result->array[6] = a * c;
    result->array[7] = a * d;
    result->array[8] = b * b;
    result->array[9] = b * c;
    result->array[10] = b * d;
    result->array[11] = c * c;
    result->array[12] = c * d;
    result->array[13] = d * d;
    PG_RETURN_POINTER(result);
}


PG_FUNCTION_INFO_V1(lift5);

Datum lift5(PG_FUNCTION_ARGS) {
    double a = (double) PG_GETARG_FLOAT8(0);
    double b = (double) PG_GETARG_FLOAT8(1);
    double c = (double) PG_GETARG_FLOAT8(2);
    double d = (double) PG_GETARG_FLOAT8(3);
    double e = (double) PG_GETARG_FLOAT8(4);


    Triple *result = (Triple *) palloc0(sizeof(Triple) + 20 * sizeof(double));
    SET_VARSIZE(result, sizeof(Triple) + 20 * sizeof(double));

    result->size = 5;
    result->count = 1;
    result->array[0] = a;
    result->array[1] = b;
    result->array[2] = c;
    result->array[3] = d;
    result->array[4] = e;
    result->array[5] = a * a;
    result->array[6] = a * b;
    result->array[7] = a * c;
    result->array[8] = a * d;
    result->array[9] = a * e;
    result->array[10] = b * b;
    result->array[11] = b * c;
    result->array[12] = b * d;
    result->array[13] = b * e;
    result->array[14] = c * c;
    result->array[15] = c * d;
    result->array[16] = c * e;
    result->array[17] = d * d;
    result->array[18] = d * e;
    result->array[19] = e * e;
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