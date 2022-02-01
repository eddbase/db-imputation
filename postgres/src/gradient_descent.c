#include "cofactor.h"
#include "postgres.h"
#include "fmgr.h"
#include <catalog/pg_type.h>
#include <math.h>
#include <assert.h>
#include "utils/array.h"
#include "relation.h"

void build_sigma_matrix(const cofactor_t *cofactor, size_t num_total_params,
                        /* out */ double *sigma)
{
    //assert(numerical_params == cofactor->num_continuous_vars + 1);
    size_t numerical_params = cofactor->num_continuous_vars + 1;
    sigma[0] = cofactor->count;

    const float8 *sum1_scalar_array = (const float8 *)cofactor->data;
    for (size_t i = 0; i < (numerical_params - 1); i++)
    {
        sigma[i + 1] = sum1_scalar_array[i];
        sigma[(i + 1) * numerical_params] = sum1_scalar_array[i];
    }

    const float8 *sum2_scalar_array = sum1_scalar_array + cofactor->num_continuous_vars;
    for (size_t row = 0; row < (numerical_params - 1); row++)
    {
        for (size_t col = 0; col < (numerical_params - 1); col++)
        {
            if (row > col)
                sigma[((row + 1) * numerical_params) + (col + 1)] = sum2_scalar_array[(col * cofactor->num_continuous_vars) - (((col) * (col + 1)) / 2) + row];
            else
                sigma[((row + 1) * numerical_params) + (col + 1)] = sum2_scalar_array[(row * cofactor->num_continuous_vars) - (((row) * (row + 1)) / 2) + col];
        }
    }

    //add relational data

    ////
    numerical_params--;
    size_t n_categorical = cofactor -> num_categorical_vars;
    
    /////

    char *relation_data = (char *)cofactor ->data + (numerical_params * sizeof(float8));
    for (size_t i = 0; i < n_categorical; i++)//each table
    {
        relation_t *r = (relation_t *)relation_data;

        //add table to sigma matrix

        r->sz_struct = sizeof_relation_t(r->num_tuples);
        relation_data += r->sz_struct;
    }

}

void compute_gradient(size_t num_params, size_t label_idx,
                      const double *sigma, const double *params,
                      /* out */ double *grad)
{
    assert(sigma[0] != 0.0);

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
    assert(sigma[0] != 0.0);

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



PG_FUNCTION_INFO_V1(linear_regression_model);

Datum linear_regression_model(PG_FUNCTION_ARGS)
{
    const cofactor_t *cofactor = (const cofactor_t *)PG_GETARG_VARLENA_P(0);
    int label = PG_GETARG_INT64(1);
    double step_size = PG_GETARG_FLOAT8(2);
    double lambda = PG_GETARG_FLOAT8(3);
    int max_num_iterations = PG_GETARG_INT64(4);

    size_t num_params = (cofactor->num_continuous_vars) + 1;

    double *grad = (double *)palloc0(sizeof(double) * num_params);
    double *prev_grad = (double *)palloc0(sizeof(double) * num_params);
    double *learned_coeff = (double *)palloc(sizeof(double) * num_params);
    double *prev_learned_coeff = (double *)palloc0(sizeof(double) * num_params);
    double *sigma = (double *)palloc0(sizeof(double) * num_params * num_params);
    double *update = (double *)palloc0(sizeof(double) * num_params);

    build_sigma_matrix(cofactor, num_params, sigma);
    for (size_t i = 0; i < num_params; i++)
        learned_coeff[i] = 0; //((double) (rand() % 800 + 1) - 400) / 100;

    prev_learned_coeff[label + 1] = -1;
    learned_coeff[label + 1] = -1;
    // elog(WARNING, "compute gradient");
    compute_gradient(num_params, label + 1, sigma, learned_coeff, grad);
    double gradient_norm = grad[0] * grad[0]; // bias
    for (size_t i = 1; i < num_params; i++)
    {
        double upd = grad[i] + lambda * learned_coeff[i];
        gradient_norm += upd * upd;
    }
    gradient_norm -= lambda * lambda; // label correction
    double first_gradient_norm = sqrt(gradient_norm);

    // elog(WARNING, "compute error");
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
        for (size_t i = 1; i < num_iterations; i++)
        {
            update[i] = grad[i] + lambda * learned_coeff[i];
            gradient_norm += update[i] * update[i];
            prev_learned_coeff[i] = learned_coeff[i];
            prev_grad[i] = grad[i];
            learned_coeff[i] = learned_coeff[i] - step_size * update[i];
            dparam_norm += update[i] * update[i];
        }
        learned_coeff[label + 1] = -1;
        gradient_norm -= lambda * lambda; // label correction
        dparam_norm = step_size * sqrt(dparam_norm);
        // elog(WARNING, "compute error %d", label+1);
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

            learned_coeff[label + 1] = -1;
            // elog(WARNING, "compute again error %d", label+1);
            error = compute_error(num_params, sigma, learned_coeff, lambda);
            backtracking_steps++;
        }

        /* Normalized residual stopping condition */
        gradient_norm = sqrt(gradient_norm);
        if (dparam_norm < 1e-20 ||
            gradient_norm / (first_gradient_norm + 0.001) < 1e-5)
        {
            break;
        }
        // elog(WARNING, "compute gradient");
        compute_gradient(num_params, label + 1, sigma, learned_coeff, grad);
        // elog(WARNING, "compute step size");
        step_size = compute_step_size(step_size, num_params, learned_coeff, prev_learned_coeff, grad, prev_grad);
        // elog(WARNING, "computed");
        prev_error = error;
        num_iterations++;
    } while (num_iterations < 1000 || num_iterations < max_num_iterations);

    // export params in pgpsql
    const double *data = learned_coeff; // C array
    Datum *d = (Datum *)palloc(sizeof(Datum) * num_params);
    for (int i = 0; i < num_params; i++)
        d[i] = Float8GetDatum(data[i]);
    ArrayType *a = construct_array(d, size_scalar_array_cont_cont(cofactor->num_continuous_vars), FLOAT8OID, sizeof(float8), true, 'd');
    PG_RETURN_ARRAYTYPE_P(a);
}
