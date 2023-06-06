//
// Created by Massimo Perini on 06/06/2023.
//

#include "Regression.h"

#include <math.h>
#include <assert.h>
#include<iostream>

struct cofactor{
    size_t N;
    size_t num_continuous_vars;
    size_t num_categorical_vars;
    std::vector<float> lin;
    std::vector<float> quad;
};

void print_matrix(size_t sz, const std::vector<double> &m)
{
    for (size_t i = 0; i < sz; i++)
    {
        for(size_t j = 0; j < sz; j++)
        {
            std::cout<<i<<", "<<j<<" -> "<< m[(i * sz) + j];
        }
    }
}

void compute_gradient(size_t num_params, size_t label_idx,
                      const std::vector<double> &sigma, const std::vector<double> &params,
        /* out */ std::vector<double> &grad)
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

double compute_error(size_t num_params, const std::vector<double> &sigma,
                     const std::vector<double> &params, const double lambda)
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
                                const std::vector<double> &params, const std::vector<double> &prev_params,
                                const std::vector<double> &grad, const std::vector<double> &prev_grad)
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

void build_sigma_matrix(const cofactor &cofactor, size_t matrix_size, int label_categorical_sigma,
        /* out */ std::vector<double> &sigma)
{
    // start numerical data:
    // numerical_params = cofactor->num_continuous_vars + 1

    // count
    sigma[0] = cofactor.N;

    // sum1
    const std::vector<float> &sum1_scalar_array = cofactor.lin;
    for (size_t i = 0; i < cofactor.num_continuous_vars; i++){
        sigma[i + 1] = sum1_scalar_array[i];
        sigma[(i + 1) * matrix_size] = sum1_scalar_array[i];
    }

    //sum2 full matrix (from half)
    const std::vector<float> &sum2_scalar_array = cofactor.quad;
    for (size_t row = 0; row < cofactor.num_continuous_vars; row++){
        for (size_t col = 0; col < cofactor.num_continuous_vars; col++){
            if (row > col)
                sigma[((row + 1) * matrix_size) + (col + 1)] = sum2_scalar_array[(col * cofactor.num_continuous_vars) - (((col) * (col + 1)) / 2) + row];
            else
                sigma[((row + 1) * matrix_size) + (col + 1)] = sum2_scalar_array[(row * cofactor.num_continuous_vars) - (((row) * (row + 1)) / 2) + col];
        }
    }
    // (numerical_params) * (numerical_params) allocated

    // start relational data:
    /*
    size_t num_categories = matrix_size - cofactor.num_continuous_vars - 1;
    uint64_t *cat_array = new uint64_t [num_categories]; // array of categories
    uint32_t *cat_vars_idxs = new uint32_t[cofactor.num_categorical_vars + 1]; // track start each cat. variable

    size_t search_start = 0;        // within one category class
    size_t search_end = search_start;

    cat_vars_idxs[0] = 0;

    const char *relation_data = crelation_array(cofactor);
    for (size_t i = 0; i < cofactor.num_categorical_vars; i++) {
        relation_t *r = (relation_t *) relation_data;
        if (label_categorical_sigma >= 0 && ((size_t)label_categorical_sigma) == i ){
            cat_vars_idxs[i + 1] = cat_vars_idxs[i];
            relation_data += r->sz_struct;
            continue;
        }
        //create sorted array
        for (size_t j = 0; j < r->num_tuples; j++) {
            size_t key_index = find_in_array(r->tuples[j].key, cat_array, search_start, search_end);
            if (key_index == search_end){
                uint64_t value_to_insert = r->tuples[j].key;
                uint64_t tmp;
                for (size_t k = search_start; k < search_end; k++){
                    if (value_to_insert < cat_array[k]){
                        tmp = cat_array[k];
                        cat_array[k] = value_to_insert;
                        value_to_insert = tmp;
                    }
                }
                cat_array[search_end] = value_to_insert;
                search_end++;
            }
        }
        search_start = search_end;
        cat_vars_idxs[i + 1] = cat_vars_idxs[i] + r->num_tuples;
        relation_data += r->sz_struct;
    }

    // count * categorical (group by A, group by B, ...)
    relation_data = crelation_array(cofactor);
    for (size_t i = 0; i < cofactor.num_categorical_vars; i++)
    {
        relation_t *r = (relation_t *) relation_data;
        if (label_categorical_sigma >= 0 && ((size_t)label_categorical_sigma) == i )
        {
            //skip this variable
            relation_data += r->sz_struct;
            continue;
        }

        for (size_t j = 0; j < r->num_tuples; j++)
        {
            // search key index
            size_t key_index = find_in_array(r->tuples[j].key, cat_array, cat_vars_idxs[i], cat_vars_idxs[i + 1]);
            assert(key_index < search_end);

            // add to sigma matrix
            key_index += cofactor->num_continuous_vars + 1;
            sigma[key_index] = r->tuples[j].value;
            sigma[key_index * matrix_size] = r->tuples[j].value;
            sigma[(key_index * matrix_size) + key_index] = r->tuples[j].value;
        }
        search_start = search_end;
        //cat_vars_idxs[i + 1] = cat_vars_idxs[i] + r->num_tuples;

        relation_data += r->sz_struct;
    }

    // categorical * numerical
    for (size_t numerical = 1; numerical < cofactor.num_continuous_vars + 1; numerical++)
    {
        for (size_t categorical = 0; categorical < cofactor.num_categorical_vars; categorical++)
        {
            relation_t *r = (relation_t *) relation_data;
            if (label_categorical_sigma >= 0 && ((size_t)label_categorical_sigma) == categorical )
            {
                //skip this variable
                relation_data += r->sz_struct;
                continue;
            }

            for (size_t j = 0; j < r->num_tuples; j++)
            {
                //search in the right categorical var
                search_start = cat_vars_idxs[categorical];
                search_end = cat_vars_idxs[categorical + 1];

                size_t key_index = find_in_array(r->tuples[j].key, cat_array, search_start, search_end);
                assert(key_index < search_end);

                // add to sigma matrix
                key_index += cofactor->num_continuous_vars + 1;
                sigma[(key_index * matrix_size) + numerical] = r->tuples[j].value;
                sigma[(numerical * matrix_size) + key_index] = r->tuples[j].value;
            }
            relation_data += r->sz_struct;
        }
    }

    // categorical * categorical
    for (size_t curr_cat_var = 0; curr_cat_var < cofactor.num_categorical_vars; curr_cat_var++)
    {
        for (size_t other_cat_var = curr_cat_var + 1; other_cat_var < cofactor.num_categorical_vars; other_cat_var++)
        {
            relation_t *r = (relation_t *) relation_data;

            if (label_categorical_sigma >= 0 && (((size_t)label_categorical_sigma) == curr_cat_var || ((size_t)label_categorical_sigma) == other_cat_var))
            {
                //skip this variable
                relation_data += r->sz_struct;
                continue;
            }

            for (size_t j = 0; j < r->num_tuples; j++)
            {
                search_start = cat_vars_idxs[curr_cat_var];
                search_end = cat_vars_idxs[curr_cat_var + 1];

                size_t key_index_curr_var = find_in_array(r->tuples[j].slots[0], cat_array, search_start, search_end);
                assert(key_index_curr_var < search_end);

                search_start = cat_vars_idxs[other_cat_var];
                search_end = cat_vars_idxs[other_cat_var + 1];

                size_t key_index_other_var = find_in_array(r->tuples[j].slots[1], cat_array, search_start, search_end);
                assert(key_index_other_var < search_end);

                // add to sigma matrix
                key_index_curr_var += cofactor->num_continuous_vars + 1;
                key_index_other_var += cofactor->num_continuous_vars + 1;
                sigma[(key_index_curr_var * matrix_size) + key_index_other_var] = r->tuples[j].value;
                sigma[(key_index_other_var * matrix_size) + key_index_curr_var] = r->tuples[j].value;
            }
            relation_data += r->sz_struct;
        }
    }

    delete [] cat_array;
    delete [] cat_vars_idxs;*/
}



size_t sizeof_sigma_matrix(const duckdb::vector<duckdb::Value> &cofactor, int label_categorical_sigma)
{
    // count :: numerical :: 1-hot_categories
    return 1 + cofactor.size();// + get_num_categories(cofactor, label_categorical_sigma);
}
/*
size_t get_num_categories(const cofactor_t *cofactor, int label_categorical_sigma)
{
    size_t num_categories = 0;

    const char *relation_data = crelation_array(cofactor);
    for (size_t i = 0; i < cofactor->num_categorical_vars; i++)
    {
        relation_t *r = (relation_t *) relation_data;

        if (label_categorical_sigma >= 0 && ((size_t)label_categorical_sigma) == i)
        {
            //skip this variable
            relation_data += r->sz_struct;
            continue;
        }

        num_categories += r->num_tuples;
        relation_data += r->sz_struct;
    }
    return num_categories;
}*/

std::vector<double> Triple::ridge_linear_regression(const duckdb::Value &triple, size_t label, double step_size, double lambda, size_t max_num_iterations)
{
    //extract data

    auto first_triple_children = duckdb::StructValue::GetChildren(triple);//vector of pointers to childrens
    cofactor cofactor;
    cofactor.N = (int) first_triple_children[0].GetValue<int>();

    duckdb::child_list_t<duckdb::Value> struct_values;
    const duckdb::vector<duckdb::Value> &linear = duckdb::ListValue::GetChildren(first_triple_children[1]);
    cofactor.lin.reserve(linear.size());
    cofactor.num_continuous_vars = linear.size();
    cofactor.num_categorical_vars = 0;//CHANGE HERE TO ADD CATEGORICAL

    for(idx_t i=0;i<linear.size();i++)
        cofactor.lin[i] = linear[i].GetValue<float>();

    const duckdb::vector<duckdb::Value> &quad = duckdb::ListValue::GetChildren(first_triple_children[2]);

    cofactor.quad.reserve(quad.size());
    for(idx_t i=0;i<quad.size();i++)
        cofactor.quad[i] = quad[i].GetValue<float>();



    if (linear.size() <= label) {
        std::cout<<"label ID >= number of continuous attributes";
        return {};
    }

    size_t num_params = sizeof_sigma_matrix(linear, -1);

    std::vector <double> grad(num_params, 0);
    std::vector <double> prev_grad(num_params, 0);
    std::vector <double> learned_coeff(num_params, 0);
    std::vector <double> prev_learned_coeff(num_params, 0);
    std::vector <double> sigma(num_params * num_params, 0);
    std::vector <double> update(num_params, 0);

    build_sigma_matrix(cofactor, num_params, -1, sigma);


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
        prev_error = error;
        num_iterations++;
    } while (num_iterations < 1000 || num_iterations < max_num_iterations);

    //std::cout<< "num_iterations = "<< num_iterations;
    // export params to pgpsql


    return learned_coeff;
}


