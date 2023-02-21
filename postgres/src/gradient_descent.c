#include "cofactor.h"
#include "relation.h"
#include <postgres.h>
#include <fmgr.h>
#include <catalog/pg_type.h>
#include <utils/array.h>
#include <math.h>
#include <assert.h>

void print_matrix(size_t sz, const double *m) 
{
    for (size_t i = 0; i < sz; i++)
    {
        for(size_t j = 0; j < sz; j++)
        {
            elog(DEBUG3, "%zu, %zu -> %f", i, j, m[(i * sz) + j]);
        }
    }
}

void compute_gradient(size_t num_params, size_t label_idx,
                      const double *sigma, const double *params,
                      /* out */ double *grad)
{
    if (sigma[0] == 0.0) return;

    /* Compute Sigma * Theta */
    for (size_t i = 0; i < num_params; i++)
    {
        grad[i] = 0.0;
        for (size_t j = 0; j < num_params; j++)
        {
            grad[i] += sigma[(i * num_params) + j] * params[j];
        }
        grad[i] /= sigma[0]; // count
    }
    grad[label_idx] = 0.0;
}

double compute_error(size_t num_params, const double *sigma,
                     const double *params, const double lambda)
{
    if (sigma[0] == 0.0) return 0.0;

    double error = 0.0;

    /* Compute 1/N * Theta^T * Sigma * Theta */
    for (size_t i = 0; i < num_params; i++)
    {
        double tmp = 0.0;
        for (size_t j = 0; j < num_params; j++)
        {
            tmp += sigma[(i * num_params) + j] * params[j];
        }
        error += params[i] * tmp;
    }
    error /= sigma[0]; // count

    /* Add the regulariser to the error */
    double param_norm = 0.0;
    for (size_t i = 1; i < num_params; i++)
    {
        param_norm += params[i] * params[i];
    }
    param_norm -= 1; // param_norm -= params[LABEL_IDX] * params[LABEL_IDX];
    error += lambda * param_norm;

    return error / 2;
}

inline double compute_step_size(double step_size, int num_params,
                                const double *params, const double *prev_params,
                                const double *grad, const double *prev_grad)
{
    double DSS = 0.0, GSS = 0.0, DGS = 0.0;

    for (int i = 0; i < num_params; i++)
    {
        double paramDiff = params[i] - prev_params[i];
        double gradDiff = grad[i] - prev_grad[i];

        DSS += paramDiff * paramDiff;
        GSS += gradDiff * gradDiff;
        DGS += paramDiff * gradDiff;
    }

    if (DGS == 0.0 || GSS == 0.0)
        return step_size;

    double Ts = DSS / DGS;
    double Tm = DGS / GSS;

    if (Tm < 0.0 || Ts < 0.0)
        return step_size;

    return (Tm / Ts > 0.5) ? Tm : Ts - 0.5 * Tm;
}

PG_FUNCTION_INFO_V1(ridge_linear_regression);

Datum ridge_linear_regression(PG_FUNCTION_ARGS)
{
    const cofactor_t *cofactor = (const cofactor_t *)PG_GETARG_VARLENA_P(0);
    size_t label = PG_GETARG_INT64(1);
    double step_size = PG_GETARG_FLOAT8(2);
    double lambda = PG_GETARG_FLOAT8(3);
    int max_num_iterations = PG_GETARG_INT64(4);

    if (cofactor->num_continuous_vars <= label) {
        elog(WARNING, "label ID >= number of continuous attributes");
        PG_RETURN_NULL();
    }

    size_t num_params = sizeof_sigma_matrix(cofactor, -1);
    
    elog(DEBUG5, "num_params = %zu", num_params);

    double *grad = (double *)palloc0(sizeof(double) * num_params);
    double *prev_grad = (double *)palloc0(sizeof(double) * num_params);
    double *learned_coeff = (double *)palloc(sizeof(double) * num_params);
    double *prev_learned_coeff = (double *)palloc0(sizeof(double) * num_params);
    double *sigma = (double *)palloc0(sizeof(double) * num_params * num_params);
    double *update = (double *)palloc0(sizeof(double) * num_params);

    build_sigma_matrix(cofactor, num_params, -1, sigma);
    print_matrix(num_params, sigma);

    for (size_t i = 0; i < num_params; i++)
    {
        learned_coeff[i] = 0; // ((double) (rand() % 800 + 1) - 400) / 100;
    }

    label += 1;     // index 0 corresponds to intercept
    prev_learned_coeff[label] = -1;
    learned_coeff[label] = -1;

    compute_gradient(num_params, label, sigma, learned_coeff, grad);
    
    double gradient_norm = grad[0] * grad[0]; // bias
    for (size_t i = 1; i < num_params; i++)
    {
        double upd = grad[i] + lambda * learned_coeff[i];
        gradient_norm += upd * upd;
    }
    gradient_norm -= lambda * lambda; // label correction
    double first_gradient_norm = sqrt(gradient_norm);

    double prev_error = compute_error(num_params, sigma, learned_coeff, lambda);

    size_t num_iterations = 1;
    do
    {
        // Update parameters and compute gradient norm
        update[0] = grad[0];
        gradient_norm = update[0] * update[0];
        prev_learned_coeff[0] = learned_coeff[0];
        prev_grad[0] = grad[0];
        learned_coeff[0] = learned_coeff[0] - step_size * update[0];
        double dparam_norm = update[0] * update[0];
        
        for (size_t i = 1; i < num_params; i++)
        {
            update[i] = grad[i] + lambda * learned_coeff[i];
            gradient_norm += update[i] * update[i];
            prev_learned_coeff[i] = learned_coeff[i];
            prev_grad[i] = grad[i];
            learned_coeff[i] = learned_coeff[i] - step_size * update[i];
            dparam_norm += update[i] * update[i];
        }
        learned_coeff[label] = -1;
        gradient_norm -= lambda * lambda; // label correction
        dparam_norm = step_size * sqrt(dparam_norm);
        
        double error = compute_error(num_params, sigma, learned_coeff, lambda);

        /* Backtracking Line Search: Decrease step_size until condition is satisfied */
        size_t backtracking_steps = 0;
        while (error > prev_error - (step_size / 2) * gradient_norm && backtracking_steps < 500)
        {
            step_size /= 2; // Update parameters based on the new step_size.

            dparam_norm = 0.0;
            for (size_t i = 0; i < num_params; i++)
            {
                double newp = prev_learned_coeff[i] - step_size * update[i];
                double dp = learned_coeff[i] - newp;
                learned_coeff[i] = newp;
                dparam_norm += dp * dp;
            }
            dparam_norm = sqrt(dparam_norm);
            learned_coeff[label] = -1;
            error = compute_error(num_params, sigma, learned_coeff, lambda);
            backtracking_steps++;
        }

        /* Normalized residual stopping condition */
        gradient_norm = sqrt(gradient_norm);
        if (dparam_norm < 1e-20 ||
            gradient_norm / (first_gradient_norm + 0.001) < 1e-8)
        {
            break;
        }
        compute_gradient(num_params, label, sigma, learned_coeff, grad);
        
        step_size = compute_step_size(step_size, num_params, learned_coeff, prev_learned_coeff, grad, prev_grad);
        
        elog(DEBUG5, "error = %f", error);
        prev_error = error;
        num_iterations++;        
    } while (num_iterations < 1000 || num_iterations < max_num_iterations);

    elog(DEBUG1, "num_iterations = %zu", num_iterations);
    elog(DEBUG1, "error = %lf", prev_error);

    // export params to pgpsql
    Datum *d = (Datum *)palloc(sizeof(Datum) * num_params);
    for (int i = 0; i < num_params; i++) 
    {
        d[i] = Float8GetDatum(learned_coeff[i]);
        elog(DEBUG2, "learned_coeff[%d] = %f", i, learned_coeff[i]);
    }

    pfree(grad);
    pfree(prev_grad);
    pfree(learned_coeff);
    pfree(prev_learned_coeff);
    pfree(sigma);
    pfree(update);

    ArrayType *a = construct_array(d, num_params, FLOAT8OID, sizeof(float8), true, 'd');
    PG_RETURN_ARRAYTYPE_P(a);
}



PG_FUNCTION_INFO_V1(ridge_linear_regression_from_params);

Datum ridge_linear_regression_from_params(PG_FUNCTION_ARGS)
{
    ArrayType *cofactor_vals = PG_GETARG_ARRAYTYPE_P(0);
    Oid arrayElementType1 = ARR_ELEMTYPE(cofactor_vals);
    int16 arrayElementTypeWidth1;
    bool arrayElementTypeByValue1;
    Datum *arrayContent1;
    bool *arrayNullFlags1;
    int arrayLength1;
    char arrayElementTypeAlignmentCode1;

    get_typlenbyvalalign(arrayElementType1, &arrayElementTypeWidth1, &arrayElementTypeByValue1, &arrayElementTypeAlignmentCode1);
    deconstruct_array(cofactor_vals, arrayElementType1, arrayElementTypeWidth1, arrayElementTypeByValue1, arrayElementTypeAlignmentCode1,
                      &arrayContent1, &arrayNullFlags1, &arrayLength1);

    size_t label = PG_GETARG_INT64(1);
    double step_size = PG_GETARG_FLOAT8(2);
    double lambda = PG_GETARG_FLOAT8(3);
    int max_num_iterations = PG_GETARG_INT64(4);

    int num_params = (-1 + sqrt(1+(4*arrayLength1)))/2;

    elog(DEBUG5, "num_params = %zu", num_params);

    double *grad = (double *)palloc0(sizeof(double) * num_params);
    double *prev_grad = (double *)palloc0(sizeof(double) * num_params);
    double *learned_coeff = (double *)palloc(sizeof(double) * num_params);
    double *prev_learned_coeff = (double *)palloc0(sizeof(double) * num_params);
    double *sigma = (double *)palloc0(sizeof(double) * num_params * num_params);
    double *update = (double *)palloc0(sizeof(double) * num_params);

    size_t row = 0;
    size_t col = 0;
    //build sigma matrix
    for (size_t i = 0; i < arrayLength1; i++){
        sigma[(num_params*row)+col] = (double) DatumGetFloat4(arrayContent1[i]);
        sigma[(num_params*col)+row] = (double) DatumGetFloat4(arrayContent1[i]);
        elog(DEBUG1, "val: = %lf", DatumGetFloat4(arrayContent1[i]);
        col++;
        if (col%num_params == 0){
            row++;
            col = row;
        }
    }
    print_matrix(num_params, sigma);

    for (size_t i = 0; i < num_params; i++)
    {
        learned_coeff[i] = 0; // ((double) (rand() % 800 + 1) - 400) / 100;
    }

    label += 1;     // index 0 corresponds to intercept
    prev_learned_coeff[label] = -1;
    learned_coeff[label] = -1;

    compute_gradient(num_params, label, sigma, learned_coeff, grad);

    double gradient_norm = grad[0] * grad[0]; // bias
    for (size_t i = 1; i < num_params; i++)
    {
        double upd = grad[i] + lambda * learned_coeff[i];
        gradient_norm += upd * upd;
    }
    gradient_norm -= lambda * lambda; // label correction
    double first_gradient_norm = sqrt(gradient_norm);

    double prev_error = compute_error(num_params, sigma, learned_coeff, lambda);

    size_t num_iterations = 1;
    do
    {
        // Update parameters and compute gradient norm
        update[0] = grad[0];
        gradient_norm = update[0] * update[0];
        prev_learned_coeff[0] = learned_coeff[0];
        prev_grad[0] = grad[0];
        learned_coeff[0] = learned_coeff[0] - step_size * update[0];
        double dparam_norm = update[0] * update[0];

        for (size_t i = 1; i < num_params; i++)
        {
            update[i] = grad[i] + lambda * learned_coeff[i];
            gradient_norm += update[i] * update[i];
            prev_learned_coeff[i] = learned_coeff[i];
            prev_grad[i] = grad[i];
            learned_coeff[i] = learned_coeff[i] - step_size * update[i];
            dparam_norm += update[i] * update[i];
        }
        learned_coeff[label] = -1;
        gradient_norm -= lambda * lambda; // label correction
        dparam_norm = step_size * sqrt(dparam_norm);

        double error = compute_error(num_params, sigma, learned_coeff, lambda);

        /* Backtracking Line Search: Decrease step_size until condition is satisfied */
        size_t backtracking_steps = 0;
        while (error > prev_error - (step_size / 2) * gradient_norm && backtracking_steps < 500)
        {
            step_size /= 2; // Update parameters based on the new step_size.

            dparam_norm = 0.0;
            for (size_t i = 0; i < num_params; i++)
            {
                double newp = prev_learned_coeff[i] - step_size * update[i];
                double dp = learned_coeff[i] - newp;
                learned_coeff[i] = newp;
                dparam_norm += dp * dp;
            }
            dparam_norm = sqrt(dparam_norm);
            learned_coeff[label] = -1;
            error = compute_error(num_params, sigma, learned_coeff, lambda);
            backtracking_steps++;
        }

        /* Normalized residual stopping condition */
        gradient_norm = sqrt(gradient_norm);
        if (dparam_norm < 1e-20 ||
            gradient_norm / (first_gradient_norm + 0.001) < 1e-8)
        {
            break;
        }
        compute_gradient(num_params, label, sigma, learned_coeff, grad);

        step_size = compute_step_size(step_size, num_params, learned_coeff, prev_learned_coeff, grad, prev_grad);

        elog(DEBUG5, "error = %f", error);
        prev_error = error;
        num_iterations++;
    } while (num_iterations < 1000 || num_iterations < max_num_iterations);

    elog(DEBUG1, "num_iterations = %zu", num_iterations);
    elog(DEBUG1, "error = %lf", prev_error);

    // export params to pgpsql
    Datum *d = (Datum *)palloc(sizeof(Datum) * num_params);
    for (int i = 0; i < num_params; i++)
    {
        d[i] = Float8GetDatum(learned_coeff[i]);
        elog(DEBUG2, "learned_coeff[%d] = %f", i, learned_coeff[i]);
    }
    elog(DEBUG1, "done...");

    pfree(grad);
    pfree(prev_grad);
    pfree(learned_coeff);
    pfree(prev_learned_coeff);
    pfree(sigma);
    //pfree(update);

    ArrayType *a = construct_array(d, num_params, FLOAT8OID, sizeof(float8), true, 'd');
    elog(DEBUG1, "returning...");

    PG_RETURN_ARRAYTYPE_P(a);
}


