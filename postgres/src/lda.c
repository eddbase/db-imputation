//
// Created by Massimo Perini on 09/02/2022.
//
#include "cofactor.h"
#include "relation.h"
#include "matrix.h"
#include <math.h>

//#include <clapack.h>
#include <float.h>

#include <fmgr.h>
#include <catalog/pg_type.h>
#include <utils/array.h>
#include <utils/lsyscache.h>

extern void dgesdd_(char *JOBZ, int *m, int *n, double *A, int *lda, double *s, double *u, int *LDU, double *vt, int *l,
             double *work, int *lwork, int *iwork, int *info);
extern void dgelsd( int* m, int* n, int* nrhs, double* a, int* lda,
                    double* b, int* ldb, double* s, double* rcond, int* rank,
                    double* work, int* lwork, int* iwork, int* info );

extern void dgemm (char *TRANSA, char* TRANSB, int *M, int* N, int *K, double *ALPHA,
                   double *A, int *LDA, double *B, int *LDB, double *BETA, double *C, int *LDC);

extern void dgemv (char *TRANSA, int *M, int* N, double *ALPHA,
                   double *A, int *LDA, double *X, int *INCX, double *BETA, double *Y, int *INCY);

// #include <postgres.h>
// #include <utils/memutils.h>
// #include <math.h>

//generate a vector of sum of attributes group by label
//returns count, sum of numerical attributes, sum of categorical attributes 1-hot encoded
void build_sum_vector(const cofactor_t *cofactor, size_t num_total_params, int label,
        /* out */ double *sum_vector, uint64_t *idx_classes)
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
            //idx_classes = (uint64_t *)palloc0(sizeof(uint64_t) * current_label->num_tuples); // array of categories

            for (size_t j=0;j<current_label->num_tuples;j++) {
                idx_classes[j] = current_label->tuples[j].key;

                search_start = cat_vars_idxs[i];
                search_end = cat_vars_idxs[i + 1];
                size_t idx_key = find_in_array(current_label->tuples[j].key, cat_array, search_start, search_end);
                assert(idx_key < search_end);
                idx_key += cofactor->num_continuous_vars + 1;

                //todo split count and remove one-hot

                //sum_vector[(j*num_total_params)+idx_key] = r->tuples[j].value;
                sum_vector[(j*num_total_params)] = r->tuples[j].value;//count

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
    const cofactor_t *cofactor = (const cofactor_t *)PG_GETARG_VARLENA_P(0);
    int label = PG_GETARG_INT64(1);

    size_t num_params = sizeof_sigma_matrix(cofactor, label);
    float8 *sigma_matrix = (float8 *)palloc0(sizeof(float8) * num_params * num_params);

    //count distinct classes in var
    size_t num_categories = 0;
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
    double *mean_vector = (double *)palloc0(sizeof(double) * num_params * num_categories);
    double *coef = (double *)palloc0(sizeof(double) * num_params * num_categories);//from mean to coeff

    uint64_t *idx_classes = (uint64_t *)palloc0(sizeof(uint64_t) * num_categories);//order of classes

    build_sigma_matrix(cofactor, num_params, label, sigma_matrix);
    build_sum_vector(cofactor, num_params, label, sum_vector, idx_classes);

    //build covariance matrix and mean vectors
    for (size_t i = 0; i < num_categories; i++) {
        for (size_t j = 0; j < num_params; j++) {
            for (size_t k = 0; k < num_params; k++) {
                sigma_matrix[(j*num_params)+k] -= ((float8)(sum_vector[(i*num_params)+j] * sum_vector[(i*num_params)+k]) / (float8) sum_vector[i*num_params]);//cofactor->count
            }
            coef[(i*num_params)+j] = sum_vector[(i*num_params)+j] / sum_vector[(i*num_params)];
            mean_vector[(j*num_categories)+i] = coef[(i*num_params)+j]; // if transposed (j*num_categories)+i
        }
    }

    //introduce shrinkage
    double shrinkage = 0.4;
    double mu = 0;
    for (size_t j = 0; j < num_params; j++) {
        mu += sigma_matrix[(j*num_params)+j];
    }
    mu /= (float) num_params;

    for (size_t j = 0; j < num_params; j++) {
        for (size_t k = 0; k < num_params; k++) {
            sigma_matrix[(j*num_params)+k] *= (1-shrinkage);//apply shrinkage part 1
        }
    }

    for (size_t j = 0; j < num_params; j++) {
        sigma_matrix[(j*num_params)+j] += shrinkage * mu;
    }

    //normalize with / count
    for (size_t j = 0; j < num_params; j++) {
        for (size_t k = 0; k < num_params; k++) {
            sigma_matrix[(j*num_params)+k] /= (float8)(cofactor->count);//or / cofactor->count - num_categories
        }
    }

    //Solve with LAPACK
    int err, lwork, rank;
    double rcond = -1.0;
    double wkopt;
    double* work;
    int *iwork = (int *) palloc(((int)((3 * num_params * log2(num_params/2)) + (11*num_params))) * sizeof(int));
    double *s = (double *) palloc(num_params * sizeof(double));

    lwork = -1;
    dgelsd( &num_params, &num_params, &num_categories, sigma_matrix, &num_params, coef, &num_params, s, &rcond, &rank, &wkopt, &lwork, iwork, &err);
    lwork = (int)wkopt;
    work = (double*)malloc( lwork*sizeof(double) );
    dgelsd( &num_params, &num_params, &num_categories, sigma_matrix, &num_params, coef, &num_params, s, &rcond, &rank, work, &lwork, iwork, &err);
    elog(WARNING, "finished with err: %d", err);

    //compute intercept

    double alpha = 1;
    double beta = 0;
    double *res = (double *)palloc(num_categories*num_categories*sizeof(double));

    char task = 'N';
    dgemm(&task, &task, &num_categories, &num_categories, &num_params, &alpha, mean_vector, &num_categories, coef, &num_params, &beta, res, &num_categories);
    elog(WARNING, "end!");
    double *intercept = (double *)palloc(num_categories*sizeof(double));
    for (size_t j = 0; j < num_categories; j++) {
        intercept[j] = (res[(j*num_categories)+j] * (-0.5)) + log(sum_vector[j * num_params] / cofactor->count);
    }

    // export in pgpsql. Return values
    Datum *d = (Datum *)palloc(sizeof(Datum) * (num_categories + num_categories + 2 + (num_params * num_categories)));

    d[0] = Float4GetDatum((float)num_params);
    d[1] = Float4GetDatum((float)num_categories);

    //returns classes order
    for (int i = 0; i < num_categories; i++) {
        d[i + 2] = Float4GetDatum((float) idx_classes[i]);
    }

    //return coefficients
    for (int i = 0; i < num_params * num_categories; i++) {
        d[i + num_categories + 2] = Float4GetDatum((float) coef[i]);
    }

    //return intercept
    for (int i = 0; i < num_categories; i++) {
        d[i + num_categories + 2 + (num_params * num_categories)] = Float4GetDatum((float) intercept[i]);
    }

    ArrayType *a = construct_array(d, (num_categories + num_categories + 2 + (num_params * num_categories)), FLOAT4OID, sizeof(float4), true, TYPALIGN_INT);
    PG_RETURN_ARRAYTYPE_P(a);
}

PG_FUNCTION_INFO_V1(lda_impute);
Datum lda_impute(PG_FUNCTION_ARGS)
{
    //todo make sure encoding (order) of features is the same of cofactor and input data
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
    int num_categories = (int) DatumGetFloat4(arrayContent1[1]);
    int *idx_classes = (int *)palloc0(sizeof(int) * num_categories);//classes order
    double *coefficients = (double *)palloc0(sizeof(double) * num_params * num_categories);
    float *intercept = (float *)palloc0(sizeof(float) * num_categories);
    double *feats_c = (double *)palloc0(sizeof(double) * arrayLength2);

    for(int i=0;i<arrayLength2;i++)
        feats_c[i] = (double) DatumGetFloat4(arrayContent2[i]);

    for(int i=0;i<num_categories;i++)
        idx_classes[i] = (int) DatumGetFloat4(arrayContent1[i+2]);

    for(int i=0;i< num_categories;i++)
        for(int j=0;j< num_params;j++)
            coefficients[(j*num_categories)+i] = (double) DatumGetFloat4(arrayContent1[(i*num_params)+j+ num_categories+2]);

    for(int i=0;i<num_categories;i++)
        intercept[i] = DatumGetFloat4(arrayContent1[i+(num_params * num_categories)+num_categories+2]);

    //end unpacking

    char task = 'N';
    double alpha = 1;
    int increment = 1;
    double beta = 0;

    double *res = (double *)palloc0(sizeof(double) * num_categories);
    dgemv(&task, &num_categories, &num_params, &alpha, coefficients, &num_categories, feats_c, &increment, &beta, res, &increment);

    double max = -DBL_MAX;
    int class = 0;

    for(int i=0;i<num_categories;i++) {
        double val = res[i] + intercept[i];
        elog(WARNING, "%lf", val);
        if (val > max)
        {
            max = val;
            class = i;
        }
    }

    PG_RETURN_INT64(idx_classes[class]);
}
