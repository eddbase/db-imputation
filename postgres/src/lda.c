//
// Created by Massimo Perini on 09/02/2022.
//
#include "cofactor.h"
#include "relation.h"
#include "matrix.h"

#include <fmgr.h>
#include <catalog/pg_type.h>
#include <utils/array.h>
#include <utils/lsyscache.h>

// #include <postgres.h>
// #include <utils/memutils.h>
// #include <math.h>

//generate a vector of sum of attributes group by label
void build_sum_vector(const cofactor_t *cofactor, size_t num_total_params, int label,
        /* out */ double *sum_vector)
{
    //allocates values in sum vector, each sum array is sorted in tuple order
    const char *relation_data = crelation_array(cofactor);
    relation_t *current_label;
    size_t num_categories = num_total_params - cofactor->num_continuous_vars - 1;
    uint64_t *cat_array = (uint64_t *)palloc0(sizeof(uint64_t) * num_categories); // array of categories, stores each key for each categorical variable
    uint32_t *cat_vars_idxs = (uint32_t *)palloc0(sizeof(uint32_t) * cofactor->num_categorical_vars + 1); // track start each cat. variable
    cat_vars_idxs[0] = 0;

    size_t search_start = 0;        // within one category class
    size_t search_end = search_start;
    uint64_t *idx_classes;

    //create order of labels of categorical values
    for (size_t i = 0; i < cofactor->num_categorical_vars; i++)
    {
        relation_t *r = (relation_t *) relation_data;
        for (size_t j = 0; j < r->num_tuples; j++)
        {
            // search key index
            size_t key_index = find_in_array(r->tuples[j].key, cat_array, search_start, search_end);
            if (key_index == search_end)    // not found
            {
                cat_array[search_end] = r->tuples[j].key;
                search_end++;
            }
        }
        cat_vars_idxs[i + 1] = cat_vars_idxs[i] + r->num_tuples;

        if (i == label) {
            current_label = r;
            idx_classes = (uint64_t *)palloc0(sizeof(uint64_t) * current_label->num_tuples); // array of categories

            for (size_t j=0;j<current_label->num_tuples;j++) {
                idx_classes[j] = current_label->tuples[j].key;

                search_start = cat_vars_idxs[i];
                search_end = cat_vars_idxs[i + 1];
                size_t idx_key = find_in_array(current_label->tuples[j].key, cat_array, search_start, search_end);
                assert(idx_key < search_end);
                idx_key += cofactor->num_continuous_vars + 1;
                sum_vector[(j*num_total_params)+idx_key] = r->tuples[j].value;
                sum_vector[(j*num_total_params)] = r->tuples[j].value;

                elog(WARNING, "Processing group by..., pos %d val %f: ", (j*num_total_params)+idx_key, r->tuples[j].value);
                elog(WARNING, "Processing group by..., pos %d val %f: ", (j*num_total_params), r->tuples[j].value);
            }
        }

        search_start = search_end;
        relation_data += r->sz_struct;
    }
    for(size_t i=0;i<num_categories;i++)
        elog(WARNING, "index %d value %d", i, cat_array[i]);


    //index of classes in label column (group by...)

    // categorical * numerical
    for (size_t numerical = 1; numerical < cofactor->num_continuous_vars + 1; numerical++)
    {
        for (size_t categorical = 0; categorical < cofactor->num_categorical_vars; categorical++)
        {
            //sum numerical group by label, copy value in sums vector

            relation_t *r = (relation_t *) relation_data;

            if (categorical != label) {
                relation_data += r->sz_struct;
                continue;
            }

            for (size_t j = 0; j < r->num_tuples; j++)
            {
                //search in the right categorical var
                //search_start = cat_vars_idxs[categorical];
                //search_end = cat_vars_idxs[categorical + 1];
                //size_t key_index = find_in_array(r->tuples[j].key, cat_array, search_start, search_end);
                size_t group_by_index = find_in_array(r->tuples[j].key, idx_classes, 0, current_label->num_tuples);
                elog(WARNING, "Processing numerical..., pos %d (numerical %d) val %f ", (group_by_index*num_total_params)+numerical, numerical, r->tuples[j].value);
                //assert(group_by_index < search_end);
                // add to sigma matrix
                sum_vector[(group_by_index*num_total_params)+numerical] = r->tuples[j].value;
                //group_by_index += cofactor->num_continuous_vars + 1;
                //sum_vector[(group_by_index*num_total_params)+numerical] = r->tuples[j].value;
            }
            relation_data += r->sz_struct;
        }
    }
    //group by categorical*categorical
    for (size_t curr_cat_var = 0; curr_cat_var < cofactor->num_categorical_vars; curr_cat_var++)
    {
        for (size_t other_cat_var = curr_cat_var + 1; other_cat_var < cofactor->num_categorical_vars; other_cat_var++)
        {
            size_t other_cat;
            int idx_other;
            int idx_current;
            relation_t *r = (relation_t *) relation_data;

            if (curr_cat_var == label) {
                other_cat = other_cat_var;
                idx_other = 1;
                idx_current = 0;
            }
            else if (other_cat_var == label){
                other_cat = curr_cat_var;
                idx_other = 0;
                idx_current = 1;
            }
            else {
                relation_data += r->sz_struct;
                continue;
            }

            for (size_t j = 0; j < r->num_tuples; j++)
            {
                search_start = cat_vars_idxs[other_cat];
                search_end = cat_vars_idxs[other_cat + 1];
                size_t key_index_other_var = find_in_array((uint64_t) r->tuples[j].slots[idx_other], cat_array, search_start, search_end);
                elog(WARNING, "Search curr cat: %d other cat %d idx found: %d search: %d start: %d end: %d", curr_cat_var, other_cat_var, key_index_other_var, r->tuples[j].slots[idx_other], search_start, search_end);

                assert(key_index_other_var < search_end);
                key_index_other_var += cofactor->num_continuous_vars + 1;
                elog(WARNING, "Processing categorical data...");

                size_t group_by_index = find_in_array(r->tuples[j].slots[idx_current], idx_classes, 0, current_label->num_tuples);
                assert(group_by_index < search_end);

                sum_vector[(group_by_index*num_total_params)+key_index_other_var] = r->tuples[j].value;
                elog(WARNING, "Processing categorical..., pos %d (categorical %d) val %f ", (group_by_index*num_total_params)+key_index_other_var, key_index_other_var, r->tuples[j].value);
            }
            relation_data += r->sz_struct;
        }
    }
}
/*
PG_FUNCTION_INFO_V1(remove_label);

Datum remove_label(PG_FUNCTION_ARGS)
{
    const cofactor_t *cofactor = (const cofactor_t *)PG_GETARG_VARLENA_P(0);
    int label = PG_GETARG_INT64(1);
    size_t num_cont = cofactor->num_continuous_vars;
    size_t num_cat = cofactor->num_categorical_vars;


    if (label < cofactor->num_continuous_vars)
        num_cont--;
    else
        num_cat--;

    size_t sz_scalar_array = size_scalar_array(num_cont);
    size_t sz_scalar_data = sz_scalar_array * sizeof(float8);
    size_t sz_relation_array = size_relation_array(num_cont, num_cat);
    size_t sz_relation_data = sz_relation_array * SIZEOF_RELATION(1);
    size_t sz_cofactor = sizeof(cofactor_t) + sz_scalar_data + sz_relation_data;
    cofactor_t *out = (cofactor_t *)palloc0(sz_cofactor);
    SET_VARSIZE(out, sz_cofactor);

    out->sz_relation_data = sz_relation_data;
    out->num_continuous_vars = num_cont;
    out->num_categorical_vars = num_cat;
    out->count = cofactor->count;

    //copy scalar values
    int j = 0;

    if (label < cofactor->num_continuous_vars) {
        for (int i = 0; i < size_scalar_array(cofactor->num_continuous_vars); i++) {
            if ((i % cofactor->num_continuous_vars) == label)
                continue;
            scalar_array(out)[j] = cscalar_array(cofactor)[i];
            j++;
        }
    }
    else {
        for (int i = 0; i < size_scalar_array(cofactor->num_continuous_vars); i++)
            scalar_array(out)[i] = cscalar_array(cofactor)[i];
    }
    //copy group by A, group by B, ... (categorical)
    if (label >= cofactor->num_continuous_vars){
        j = 0;
        for (int i = 0; i < cofactor->num_categorical_vars; i++) {
            if((i + cofactor->num_continuous_vars) == label)
                continue;
            relation_array(out)[j] = crelation_array(cofactor)[i];
            j++;
        }
    }
    else{
        for (int i = 0; i < cofactor->num_categorical_vars; i++)
            relation_array(out)[i] = crelation_array(cofactor)[i];
    }

    //copy cont*categorical
    //numerical1*cat1, numerical1*cat2,  numerical2*cat1, numerical2*cat2
    for (size_t numerical = 1; numerical < cofactor->num_continuous_vars+1; numerical++) {
        for (size_t categorical = 0; categorical < cofactor->num_categorical_vars; categorical++) {

        }}
            //copy cat*cat
    //pairs (e.g., GROUP BY A,B, A,C, B,C)

    PG_RETURN_POINTER(out);
}

*/
//input: triple. Output: sum vector concat. with covariance matrix
PG_FUNCTION_INFO_V1(lda_train);

Datum lda_train(PG_FUNCTION_ARGS)
{
    elog(NOTICE, "TEST1");
    const cofactor_t *cofactor = (const cofactor_t *)PG_GETARG_VARLENA_P(0);
    int label = PG_GETARG_INT64(1);

    size_t num_params = sizeof_sigma_matrix(cofactor);
    double *sigma_matrix = (double *)palloc0(sizeof(double) * num_params * num_params);
    //count distinct classes

    size_t num_categories = 0;
    //find right relation (where categorical variable is the label)
    //relation starts from count * categorical (group by A, group by B, ...), so get the right group by and count tuples in it
    const char *relation_data = crelation_array(cofactor);
    for (size_t i = 0; i < cofactor->num_categorical_vars; i++)
    {
        relation_t *r = (relation_t *) relation_data;
        if (i == label) {
            //count unique keys
            num_categories = r->num_tuples;
            break;
        }
        relation_data += r->sz_struct;
    }

    double *sum_vector = (double *)palloc0(sizeof(double) * num_params * num_categories);
    //build X^T*X
    build_sigma_matrix(cofactor, num_params, sigma_matrix);
    build_sum_vector(cofactor, num_params, label, sum_vector);

    for(size_t i=0;i<num_categories;i++)
    {
        elog(WARNING, "row %d values: ", i);
        for(size_t j=0;j<num_params;j++)
            elog(WARNING, " %f , ", sum_vector[(i*num_params)+j]);
    }

    //compute covariance
    for (size_t i = 0; i < num_categories; i++) {
        for (size_t j = 0; i < num_params; i++) {
            for (size_t k = 0; i < num_params; i++) {
                sigma_matrix[(j*num_params)+k] -= ((float)(sum_vector[(i*num_params)+j] * sum_vector[(i*num_params)+k]) / (float) sum_vector[i*num_params]);//cofactor->count
            }
        }
    }

    for (size_t j = 0; i < num_params; i++) {
        for (size_t k = 0; i < num_params; i++) {
            sigma_matrix[(j*num_params)+k] /= (cofactor->count - num_categories);
        }
    }
    double *inverted_matrix = (double *)palloc0(sizeof(double) * num_params * num_params);
    m_inverse(sigma_matrix, num_params, inverted_matrix);

            // export in pgpsql. Return counts, means and covariance
    Datum *d = (Datum *)palloc(sizeof(Datum) * ((num_params * num_params) + (num_params * num_categories) + num_categories + 2));
    d[0] = num_params;
    d[1] = num_categories;

    for (int i = 0; i < num_categories; i++)
        d[i+2] = Float4GetDatum((float)sum_vector[i*num_params] / (float)cofactor->count);

    for (int i = 0; i < num_categories; i++)
        for (int j = 0; j < num_params; j++)
            d[i + num_categories + 2] = Float4GetDatum((float)sum_vector[(i*num_params)+j]/(float)sum_vector[i*num_params]);

    for (int i = 0; i < num_params * num_params; i++)
        d[i + (num_params * num_categories) + num_categories + 2] = Float4GetDatum(inverted_matrix[i]);

    ArrayType *a = construct_array(d, ((num_params * num_params) + (num_params * num_categories) + num_categories), FLOAT4OID, sizeof(float4), true, 'd');
    PG_RETURN_ARRAYTYPE_P(a);
}

PG_FUNCTION_INFO_V1(lda_impute);
Datum lda_impute(PG_FUNCTION_ARGS)
{
    ArrayType *means_covariance = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *feats = PG_GETARG_ARRAYTYPE_P(1);

    Oid arrayElementType1 = ARR_ELEMTYPE(means_covariance);
    Oid arrayElementType2 = ARR_ELEMTYPE(feats);

    int16 arrayElementTypeWidth1, arrayElementTypeWidth2;
    bool arrayElementTypeByValue1, arrayElementTypeByValue2;
    Datum *arrayContent1, *arrayContent2;
    bool *arrayNullFlags1, *arrayNullFlags2;
    int arrayLength1, arrayLength2;
    char arrayElementTypeAlignmentCode1, arrayElementTypeAlignmentCode2;

    get_typlenbyvalalign(arrayElementType1, &arrayElementTypeWidth1, &arrayElementTypeByValue1, &arrayElementTypeAlignmentCode1);
    deconstruct_array(means_covariance, arrayElementType1, arrayElementTypeWidth1, arrayElementTypeByValue1, arrayElementTypeAlignmentCode1,
                      &arrayContent1, &arrayNullFlags1, &arrayLength1);

    get_typlenbyvalalign(arrayElementType2, &arrayElementTypeWidth2, &arrayElementTypeByValue2, &arrayElementTypeAlignmentCode2);
    deconstruct_array(feats, arrayElementType2, arrayElementTypeWidth2, arrayElementTypeByValue2, arrayElementTypeAlignmentCode2,
                      &arrayContent2, &arrayNullFlags2, &arrayLength2);

    int num_params = (int) DatumGetFloat4(arrayContent1[0]);
    int num_categories = (int) DatumGetFloat4(arrayContent2[1]);
    //counts
    double *counts = (double *)palloc0(sizeof(double) * num_categories);//count grp by column / total rows
    double *means = (double *)palloc0(sizeof(double) * num_categories * num_params);//mean of feature for every class
    double *covariance = (double *)palloc0(sizeof(double) * num_params * num_params);

    double *feats = (double *)palloc0(num_params*sizeof(float32));

    for(int i=0;i<num_categories;i++)
        counts[i] = DatumGetFloat4(arrayContent1[i+2]);
    for(int i=0;i<num_categories*num_params;i++)
        means[i] = DatumGetFloat4(arrayContent1[i+num_categories+2]);
    for(int i=0;i<num_params*num_params;i++)
        covariance[i] = DatumGetFloat4(arrayContent1[i+num_categories+(num_categories*num_params)+2]);

    for(int i=0;i<arrayLength2;i++)
        feats[i] = DatumGetFloat4(arrayContent2[i]);//todo manual padding of categorical values

    //end unpacking

    float32 *tmp = (float32 *) palloc0(num_params*sizeof(float32));
    float32 tmp2;
    float32 tmp3;
    float32 maximize = -99;
    int max_class = 0;

    for(int i=0;i<num_categories;i++)
    {
        //compute prob. features in this class
        float32 log_part = logf(counts[i]);
        m_mul(&(means[i*num_params]), 1, num_params, covariance, num_params, num_params, tmp);//mean*cov.*mean
        m_mul(tmp, 1, num_params, &(means[i*num_params]), num_params, 1, &tmp2);//mean*cov.*mean
        //todo features must have same encoding of cofactor
        m_mul(feats, 1, num_params, covariance, num_params, num_params, tmp);//mean*cov.*mean
        m_mul(tmp, 1, num_params, &(means[i*num_params]), num_params, 1, &tmp3);//mean*cov.*mean

        if ((log_part + tmp2 + tmp3) > maximize){
            maximize = log_part + tmp2 + tmp3;
            max_class = i;//todo clarify class
        }
    }

    PG_RETURN_INT64(max_class);
}
