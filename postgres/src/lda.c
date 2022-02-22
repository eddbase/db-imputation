//
// Created by Massimo Perini on 09/02/2022.
//
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
    const char *relation_data = crelation_array(cofactor);
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
    const float8 *sum1_scalar_array = (const float8 *)cofactor->data;
    const float8 *sum2_scalar_array = sum1_scalar_array + cofactor->num_continuous_vars;
    for (size_t row = 0; row < (numerical_params - 1); row++)
    {
        for (size_t col = 0; col < (numerical_params - 1); col++)
        {
            if (row > col)
                sigma[((row) * (numerical_params + total_keys)) + (col)] = sum2_scalar_array[(col * cofactor->num_continuous_vars) - (((col) * (col + 1)) / 2) + row];
            else
                sigma[((row) * (numerical_params + total_keys)) + (col)] = sum2_scalar_array[(row * cofactor->num_continuous_vars) - (((row) * (row + 1)) / 2) + col];
        }
    }
    //(numerical_params)*(numerical_params) allocated
    //add relational data
    /////
    relation_data = crelation_array(cofactor);
    uint64_t *key_idxs = (uint64_t *)palloc0(sizeof(uint64_t) * total_keys); //keep keys idxs
    uint64_t *cat_vars_idxs = (uint64_t *)palloc0(sizeof(uint64_t) * cofactor->num_categorical_vars + 1);//track start each cat. variable
    size_t search_start = 0;
    size_t search_end = search_start;
    cat_vars_idxs[0] = 0;

    //allocate group by A, group by B, ...
    for (size_t i = 0; i < cofactor->num_categorical_vars; i++)//group by A,B,... (diagonal)
    {
        relation_t *r = (relation_t *) relation_data;
        for (size_t j = 0; j < r->num_tuples; j++) {
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
            key_index += cofactor->num_continuous_vars + 1;

            //key is key_index;
            sigma[(key_index * (total_keys + numerical_params)) + key_index] = r->tuples[j].value;
            sigma[key_index] = r->tuples[j].value;
            sigma[key_index * (total_keys + numerical_params)] = r->tuples[j].value;
        }
        search_start = search_end;
        cat_vars_idxs[i+1] = cat_vars_idxs[i] + r->num_tuples;

        r->sz_struct = sizeof_relation_t(r->num_tuples);
        relation_data += r->sz_struct;
    }
    //cat * numerical
    //numerical1*cat1, numerical1*cat2,  numerical2*cat1, numerical2*cat2
    for (size_t numerical = 1; numerical < cofactor->num_continuous_vars+1; numerical++) {
        for (size_t categorical = 0; categorical < cofactor->num_categorical_vars; categorical++) {
            relation_t *r = (relation_t *) relation_data;

            for (size_t j = 0; j < r->num_tuples; j++) {
                //elog(NOTICE, "cofactor numeric-class: (%zu, %lu) -> %f", numerical, r->tuples[j].key,
                //     r->tuples[j].value);
                //search in the right categorical var
                search_start = cat_vars_idxs[categorical];
                search_end = cat_vars_idxs[categorical + 1];

                size_t key_index_curr_var = search_start;
                while (key_index_curr_var < search_end) {//search in the keys of this col
                    if (key_idxs[key_index_curr_var] == r->tuples[j].key)
                        break;
                    key_index_curr_var++;
                }

                key_index_curr_var += cofactor->num_continuous_vars + 1;
                sigma[(key_index_curr_var * (total_keys + numerical_params)) +
                      numerical] = r->tuples[j].value;
                sigma[(numerical * (total_keys + numerical_params)) +
                      key_index_curr_var] = r->tuples[j].value;
            }
            r->sz_struct = sizeof_relation_t(r->num_tuples);
            relation_data += r->sz_struct;
        }
    }

    size_t curr_cat_var = 0;//e.g, A
    size_t other_cat_var = 1;//e.g, B

    //pairs (e.g., GROUP BY A,B, A,C, B,C)
    for (size_t i = 0; i < size_relation_array_cat_cat(cofactor->num_categorical_vars); i++) {
        //elog(NOTICE, "relation: %zu", i);
        relation_t *r = (relation_t *) relation_data;
        for (size_t j = 0; j < r->num_tuples; j++) {
            //elog(NOTICE, "cofactor joined keys: (%u, %u) -> %f", r->tuples[j].slots[0], r->tuples[j].slots[1],
            //     r->tuples[j].value);
            //slot[0] current var, slot 1 other var
            search_start = cat_vars_idxs[curr_cat_var];
            search_end = cat_vars_idxs[curr_cat_var+1];

            size_t key_index_curr_var = search_start;

            while (key_index_curr_var < search_end){//search in the keys of this col
                if (key_idxs[key_index_curr_var] == r->tuples[j].slots[0])
                    break;
                key_index_curr_var++;
            }
            search_start = cat_vars_idxs[other_cat_var];
            search_end = cat_vars_idxs[other_cat_var + 1];

            size_t key_index_other_var = search_start;
            while (key_index_other_var < search_end) {
                if (key_idxs[key_index_other_var] == r->tuples[j].slots[1])
                    break;
                key_index_other_var++;
            }

            key_index_curr_var += cofactor->num_continuous_vars + 1;
            key_index_other_var += cofactor->num_continuous_vars + 1;

            //elog(NOTICE, "cofactor joined keys: (%u, %u) -> (%zu, %zu)", r->tuples[j].slots[0], r->tuples[j].slots[1],key_index_curr_var, key_index_other_var);

            sigma[(key_index_curr_var * (total_keys + numerical_params)) + key_index_other_var] = r->tuples[j].value;
            sigma[(key_index_other_var * (total_keys + numerical_params)) + key_index_curr_var] = r->tuples[j].value;

        }
        //elog(NOTICE, "current cat %zu , other cat %zu", curr_cat_var, other_cat_var);
        other_cat_var++;
        if (other_cat_var == cofactor->num_categorical_vars){
            curr_cat_var++;
            other_cat_var = curr_cat_var+1;
        }
        //elog(NOTICE, "current cat %zu , other cat %zu", curr_cat_var, other_cat_var);
        r->sz_struct = sizeof_relation_t(r->num_tuples);
        relation_data += r->sz_struct;
    }

    for (size_t i=0;i<(total_keys+numerical_params);i++)
        for(size_t j=0;j<(total_keys+numerical_params);j++)
            elog(NOTICE, "%zu, %zu -> %f", i, j, sigma[(i*(numerical_params+total_keys)) + j]);
}



PG_FUNCTION_INFO_V1(lda);
Datum lda(PG_FUNCTION_ARGS)
{
    elog(NOTICE, "TEST1");
    const cofactor_t *cofactor = (const cofactor_t *)PG_GETARG_VARLENA_P(0);
    int label = PG_GETARG_INT64(1);

    size_t num_params = (cofactor->num_continuous_vars) + 1;
    const char *relation_data = crelation_array(cofactor);
    //count classes and add to num_params
    for (size_t i = 0; i < cofactor->num_categorical_vars; i++)//n. classes for each variable (scan GRP BY A, GRP BY B, ...)
    {
        relation_t *r = (relation_t *) relation_data;
        num_params += r->num_tuples;
        //index keys
        r->sz_struct = sizeof_relation_t(r->num_tuples);
        relation_data += r->sz_struct;
    }
    elog(NOTICE, "num_params %zu", num_params);

    double *sums = (double *)palloc0(sizeof(double) * num_params);
    double *sigma_matrix = (double *)palloc0(sizeof(double) * num_params * num_params);
    double count = cofactor->count;

    build_sigma_matrix(cofactor, num_params, sigma_matrix);
}
