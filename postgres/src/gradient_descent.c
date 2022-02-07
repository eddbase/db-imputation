#include "cofactor.h"
#include "postgres.h"
#include "fmgr.h"
#include <catalog/pg_type.h>
#include <math.h>
#include <assert.h>
#include "utils/array.h"
#include "relation.h"

#include "hashmap.h"

void build_sigma_matrix(const cofactor_t *cofactor, size_t num_total_params,
                        /* out */ double *sigma)
{
    //count categorical
    char *relation_data = (char *)cofactor ->data + (cofactor->num_continuous_vars * sizeof(float8));
    elog(NOTICE, "%d %d", cofactor->num_continuous_vars, cofactor ->num_categorical_vars);
    size_t total_keys = 0;
    //compute total keys
    for (size_t i = 0; i < cofactor->num_categorical_vars; i++)//group by A,B,... (diagonal)
    {
        relation_t *r = (relation_t *) relation_data;
        total_keys += r->num_tuples;
        r->sz_struct = sizeof_relation_t(r->num_tuples);
        relation_data += r->sz_struct;
    }
    //start numerical

    size_t numerical_params = cofactor->num_continuous_vars + 1;
    sigma[0] = cofactor->count;

    const float8 *sum1_scalar_array = (const float8 *)cofactor->data;
    for (size_t i = 0; i < (numerical_params - 1); i++)
    {
        sigma[i + 1] = sum1_scalar_array[i];
        sigma[(i + 1) * (numerical_params + total_keys)] = sum1_scalar_array[i];
    }

    const float8 *sum2_scalar_array = sum1_scalar_array + cofactor->num_continuous_vars;
    for (size_t row = 0; row < (numerical_params - 1); row++)
    {
        for (size_t col = 0; col < (numerical_params - 1); col++)
        {
            if (row > col)
                sigma[((row + 1) * (numerical_params + total_keys)) + (col + 1)] = sum2_scalar_array[(col * cofactor->num_continuous_vars) - (((col) * (col + 1)) / 2) + row];
            else
                sigma[((row + 1) * (numerical_params + total_keys)) + (col + 1)] = sum2_scalar_array[(row * cofactor->num_continuous_vars) - (((row) * (row + 1)) / 2) + col];
        }
    }

    //(numerical_params)*(numerical_params) allocated
    //add relational data

    /////

    uint64_t *key_idxs = (uint64_t *)palloc0(sizeof(uint64_t) * total_keys); //keep keys idxs
    uint64_t *cat_vars_idxs = (uint64_t *)palloc0(sizeof(uint64_t) * cofactor->num_categorical_vars + 1);//track start each cat. variable

    size_t search_start = 0;
    size_t search_end = search_start;
    //size_t offset = numerical_params+1;

    cat_vars_idxs[0] = 0;

    //allocate group by A, group by B, ...

    for (size_t i = 0; i < cofactor->num_categorical_vars; i++)//group by A,B,... (diagonal)
    {
        relation_t *r = (relation_t *) relation_data;
        for (size_t j = 0; j < r->num_tuples; j++) {
            elog(NOTICE, "cofactor %zu: %lu -> %f", i, r->tuples[j].key, r->tuples[j].value);
            //search key index
            size_t key_index = search_start;
            while (key_index < search_end){
                if (key_idxs[key_index] == r->tuples[j].key)
                    break;
                key_index++;
            }

            if (key_index == search_end){//not found
                key_idxs[search_end] = r->tuples[j].key;
                search_end++;
            }

            //key is key_index;
            sigma[(key_index * (total_keys + numerical_params)) + key_index] = r->tuples[j].value;
            elog(NOTICE, "key: %lu -> %zu", r->tuples[j].key, key_index);
        }
        //offset += (r->num_tuples * r->num_tuples);
        search_start = search_end;
        cat_vars_idxs[i+1] = cat_vars_idxs[i] + r->num_tuples;

        r->sz_struct = sizeof_relation_t(r->num_tuples);
        relation_data += r->sz_struct;
    }


    //pairs (e.g., GROUP BY A,B, A,C, B,C)
    for (size_t i = 0; i < (cofactor->num_categorical_vars)*((cofactor->num_categorical_vars-1)/2); i++) {
        elog(NOTICE, "pairs %zu", i);

        size_t curr_cat_var = 0;//e.g, A
        size_t other_cat_var = 1;//e.g, B

        relation_t *r = (relation_t *) relation_data;
        for (size_t j = 0; j < r->num_tuples; j++) {
            elog(NOTICE, "cofactor joined keys: (%u, %u) -> %f", r->tuples[j].slots[0], r->tuples[j].slots[1],
                 r->tuples[j].value);
            //slot[0] current var, slot 1 other var
            search_start = cat_vars_idxs[curr_cat_var];
            search_end = cat_vars_idxs[curr_cat_var+1];

            size_t key_index_curr_var = search_start;
            while (key_index_curr_var < search_end){
                if (key_idxs[key_index_curr_var] == r->tuples[j].slots[0])
                    break;
                key_index_curr_var++;
            }

            search_start = cat_vars_idxs[other_cat_var];
            search_end = cat_vars_idxs[other_cat_var+1];

            size_t key_index_other_var = search_start;
            while (key_index_other_var < search_end){
                if (key_idxs[key_index_other_var] == r->tuples[j].slots[1])
                    break;
                key_index_other_var++;
            }

            sigma[(key_index_curr_var * (total_keys + numerical_params)) + key_index_other_var] = r->tuples[j].value;
            sigma[(key_index_other_var * (total_keys + numerical_params)) + key_index_curr_var] = r->tuples[j].value;

        }
        
        other_cat_var++;
        if (other_cat_var > cofactor->num_categorical_vars){
            curr_cat_var++;
            other_cat_var = curr_cat_var+1;
        }

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
    elog(NOTICE, "TEST1");
    const cofactor_t *cofactor = (const cofactor_t *)PG_GETARG_VARLENA_P(0);
    int label = PG_GETARG_INT64(1);
    double step_size = PG_GETARG_FLOAT8(2);
    double lambda = PG_GETARG_FLOAT8(3);
    int max_num_iterations = PG_GETARG_INT64(4);

    size_t num_params = (cofactor->num_continuous_vars) + 1;

    //generate hashmap classes
    //struct hashmap *map = hashmap_new(sizeof(tuple_t), 0, 0, 0,user_hash, user_compare, NULL, NULL);

    //get and sort keys
    char *relation_data = (char *)cofactor ->data + (cofactor->num_continuous_vars * sizeof(float8));
    elog(NOTICE, "num_params %zu", num_params);
    //count classes and add to num_params
    for (size_t i = 0; i < cofactor->num_categorical_vars; i++)//n. classes for each variable (scan GRP BY A, GRP BY B, ...)
    {
        relation_t *r = (relation_t *) relation_data;
        elog(NOTICE, "num_tuples %u", r->num_tuples);
        num_params += r->num_tuples;
        //index keys
        r->sz_struct = sizeof_relation_t(r->num_tuples);
        relation_data += r->sz_struct;
    }
    elog(NOTICE, "num_params %zu", num_params);
    //uint64_t sorted_keys = malloc(r->num_tuples * sizeof(uint64_t));
    //for(size_t j = 0; j < )
    //r ->tuples

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
