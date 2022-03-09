//
// Created by Massimo Perini on 09/02/2022.
//
#include "cofactor.h"
#include "postgres.h"
#include "fmgr.h"
#include <catalog/pg_type.h>
#include "relation.h"
#include "hashmap.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "matrix.h"
#include <math.h>

void build_cov_matrix(const cofactor_t *cofactor, size_t num_total_params,
        /* out */ double *sigma, size_t total_keys)
{
    //start numerical

    size_t numerical_params = cofactor->num_continuous_vars;
    const float8 *sum1_scalar_array = (const float8 *)cofactor->data;
    const float8 *sum2_scalar_array = sum1_scalar_array + cofactor->num_continuous_vars;
    for (size_t row = 0; row < numerical_params; row++)
    {
        for (size_t col = 0; col < numerical_params; col++)
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
    const char *relation_data = crelation_array(cofactor);
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
            key_index += cofactor->num_continuous_vars;//todo +1?

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
    for (size_t numerical = 0; numerical < cofactor->num_continuous_vars; numerical++) {
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

                key_index_curr_var += cofactor->num_continuous_vars;
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

            key_index_curr_var += cofactor->num_continuous_vars;
            key_index_other_var += cofactor->num_continuous_vars;
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

void build_sum_vector(const cofactor_t *cofactor, size_t num_total_params,
        /* out */ double *sum_vector, size_t total_keys)
{
    const char *relation_data = crelation_array(cofactor);
    size_t numerical_params = cofactor->num_continuous_vars;
    const float8 *sum1_scalar_array = (const float8 *)cofactor->data;

    for (size_t i = 0; i < (numerical_params); i++)
        sum_vector[i] = sum1_scalar_array[i];

    size_t search_start = 0;
    size_t search_end = search_start;
    uint64_t *key_idxs = (uint64_t *)palloc0(sizeof(uint64_t) * total_keys); //keep keys idxs
    uint64_t *cat_vars_idxs = (uint64_t *)palloc0(sizeof(uint64_t) * cofactor->num_categorical_vars + 1);//track start each cat. variable
    cat_vars_idxs[0] = 0;

    for (size_t i = 0; i < cofactor->num_categorical_vars; i++)
    {
        relation_t *r = (relation_t *) relation_data;
        for (size_t j = 0; j < r->num_tuples; j++) {
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
            key_index += cofactor->num_continuous_vars;
            sum_vector[key_index] = r->tuples[j].value;
        }
        search_start = search_end;
        cat_vars_idxs[i+1] = cat_vars_idxs[i] + r->num_tuples;

        r->sz_struct = sizeof_relation_t(r->num_tuples);
        relation_data += r->sz_struct;
    }

}

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
    size_t sz_relation_data = sz_relation_array * SIZEOF_RELATION_1;
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


            //copy cat*cat
    //pairs (e.g., GROUP BY A,B, A,C, B,C)

    PG_RETURN_POINTER(out);
}


//input: triple. Output: sum vector concat. with covariance matrix
PG_FUNCTION_INFO_V1(lda_train);
Datum lda_train(PG_FUNCTION_ARGS)
{
    elog(NOTICE, "TEST1");
    const cofactor_t *cofactor = (const cofactor_t *)PG_GETARG_VARLENA_P(0);
    //int label = PG_GETARG_INT64(1);

    size_t num_params = (cofactor->num_continuous_vars);
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

    double *sigma_matrix = (double *)palloc0(sizeof(double) * num_params * num_params);
    double *sum_vector = (double *)palloc0(sizeof(double) * num_params);

    build_cov_matrix(cofactor, num_params, sigma_matrix, num_params);
    build_sum_vector(cofactor, num_params, sum_vector, num_params);
    //compute covariance
    for (size_t i = 0; i < num_params * num_params; i++) {
        sigma_matrix[i] -= (sum_vector[(int)(i/num_params)] * sum_vector[i%num_params])/cofactor->count;
    }
    // export in pgpsql
    Datum *d = (Datum *)palloc(sizeof(Datum) * ((num_params * num_params) + num_params));
    for (int i = 0; i < num_params; i++)
        d[i] = Float4GetDatum(sum_vector[i]);

    for (int i = 0; i < num_params * num_params; i++)
        d[i + num_params] = Float4GetDatum(sigma_matrix[i]);

    ArrayType *a = construct_array(d, ((num_params * num_params) + num_params), FLOAT4OID, sizeof(float4), true, 'd');
    PG_RETURN_ARRAYTYPE_P(a);
}
//Input; covariance matrix (postgres). Output: inverse of the covariance matrix (postgres)
PG_FUNCTION_INFO_V1(lda_invert);
Datum lda_invert(PG_FUNCTION_ARGS)
{
    ArrayType *covariance = PG_GETARG_ARRAYTYPE_P(0);

    Oid arrayElementType1 = ARR_ELEMTYPE(covariance);
    int16 arrayElementTypeWidth1;
    bool arrayElementTypeByValue1;
    char arrayElementTypeAlignmentCode1;
    Datum *arrayContent1;
    bool *arrayNullFlags1;
    int arrayLength1;

    get_typlenbyvalalign(arrayElementType1, &arrayElementTypeWidth1, &arrayElementTypeByValue1, &arrayElementTypeAlignmentCode1);
    deconstruct_array(covariance, arrayElementType1, arrayElementTypeWidth1, arrayElementTypeByValue1, arrayElementTypeAlignmentCode1,
                      &arrayContent1, &arrayNullFlags1, &arrayLength1);

    float32 *matrix = malloc(arrayLength1 * sizeof(float32));
    float32 *m_inv = malloc(arrayLength1 * sizeof(float32));
    //unpack matrix
    for (int i=0;i<arrayLength1;i++)
        matrix[i] = DatumGetFloat4(arrayContent1[i]);

    m_inverse(matrix, arrayLength1, m_inv);
    Datum *d = (Datum *)palloc(sizeof(Datum) * arrayLength1);

    for (int i = 0; i < arrayLength1; i++)
        d[i] = Float4GetDatum(m_inv[i]);

    ArrayType *a = construct_array(d, (arrayLength1), FLOAT4OID, sizeof(float4), true, 'd');

    free(matrix);
    free(m_inv);

    PG_RETURN_ARRAYTYPE_P(a);
}


PG_FUNCTION_INFO_V1(lda_impute);
Datum lda_impute(PG_FUNCTION_ARGS)
{
    ArrayType *means = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *covariance = PG_GETARG_ARRAYTYPE_P(1);
    ArrayType *probs_classes = PG_GETARG_ARRAYTYPE_P(2);
    ArrayType *feats = PG_GETARG_ARRAYTYPE_P(3);


    Oid arrayElementType1 = ARR_ELEMTYPE(means);
    Oid arrayElementType2 = ARR_ELEMTYPE(covariance);
    Oid arrayElementType3 = ARR_ELEMTYPE(probs_classes);
    Oid arrayElementType4 = ARR_ELEMTYPE(feats);


    int16 arrayElementTypeWidth1, arrayElementTypeWidth2, arrayElementTypeWidth3, arrayElementTypeWidth4;
    bool arrayElementTypeByValue1, arrayElementTypeByValue2, arrayElementTypeByValue3, arrayElementTypeByValue4;
    char arrayElementTypeAlignmentCode1, arrayElementTypeAlignmentCode2, arrayElementTypeAlignmentCode3, arrayElementTypeAlignmentCode4;
    Datum *arrayContent1, *arrayContent2, *arrayContent3, *arrayContent4;
    bool *arrayNullFlags1, *arrayNullFlags2, *arrayNullFlags3, *arrayNullFlags4;
    int arrayLength1, arrayLength2, arrayLength3, arrayLength4;

    get_typlenbyvalalign(arrayElementType1, &arrayElementTypeWidth1, &arrayElementTypeByValue1, &arrayElementTypeAlignmentCode1);
    deconstruct_array(means, arrayElementType1, arrayElementTypeWidth1, arrayElementTypeByValue1, arrayElementTypeAlignmentCode1,
                      &arrayContent1, &arrayNullFlags1, &arrayLength1);

    get_typlenbyvalalign(arrayElementType2, &arrayElementTypeWidth2, &arrayElementTypeByValue2, &arrayElementTypeAlignmentCode2);
    deconstruct_array(covariance, arrayElementType2, arrayElementTypeWidth2, arrayElementTypeByValue2, arrayElementTypeAlignmentCode2,
                      &arrayContent2, &arrayNullFlags2, &arrayLength2);

    get_typlenbyvalalign(arrayElementType3, &arrayElementTypeWidth3, &arrayElementTypeByValue3, &arrayElementTypeAlignmentCode3);
    deconstruct_array(probs_classes, arrayElementType3, arrayElementTypeWidth3, arrayElementTypeByValue3, arrayElementTypeAlignmentCode3,
                      &arrayContent3, &arrayNullFlags3, &arrayLength3);

    get_typlenbyvalalign(arrayElementType4, &arrayElementTypeWidth4, &arrayElementTypeByValue4, &arrayElementTypeAlignmentCode4);
    deconstruct_array(feats, arrayElementType4, arrayElementTypeWidth4, arrayElementTypeByValue4, arrayElementTypeAlignmentCode4,
                      &arrayContent4, &arrayNullFlags4, &arrayLength4);

    float32 *inv_covariance_c = malloc(arrayLength2*sizeof(float32));
    float32 *means_c = malloc(arrayLength1*sizeof(float32));
    float32 *feats_c = malloc(arrayLength4*sizeof(float32));

    for(int i=0;i<arrayLength2;i++)
        inv_covariance_c[i] = DatumGetFloat4(arrayContent2[i]);
    for(int i=0;i<arrayLength1;i++)
        means_c[i] = DatumGetFloat4(arrayContent1[i]);
    for(int i=0;i<arrayLength4;i++)
        feats_c[i] = DatumGetFloat4(arrayContent4[i]);


    int classes = arrayLength3;
    int attrs = sqrt(arrayLength2);

    float32 *tmp = malloc(attrs*sizeof(float32));
    float32 tmp2;
    float32 tmp3;
    float32 maximize = -99;
    int max_class = 0;

    for(int i=0;i<classes;i++)
    {
        //compute prob. features in this class
        float32 log_part = logf(DatumGetFloat4(arrayContent3[i]));
        m_mul(&(means_c[i*attrs]), 1, attrs, inv_covariance_c, attrs, attrs, tmp);//mean*cov.*mean
        m_mul(tmp, 1, attrs, &(means_c[i*attrs]), attrs, 1, &tmp2);//mean*cov.*mean

        m_mul(feats_c, 1, attrs, inv_covariance_c, attrs, attrs, tmp);//mean*cov.*mean
        m_mul(tmp, 1, attrs, &(means_c[i*attrs]), attrs, 1, &tmp3);//mean*cov.*mean

        if ((log_part + tmp2 + tmp3) > maximize){
            maximize = log_part + tmp2 + tmp3;
            max_class = i;
        }
    }

    free(inv_covariance_c);
    free(means_c);
    free(feats_c);
    free(tmp);

    PG_RETURN_INT64(max_class);
}
