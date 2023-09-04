#include "cofactor.h"
#include "relation.h"
#include <math.h>

//#include <clapack.h>
#include <float.h>

#include <fmgr.h>
#include <catalog/pg_type.h>
#include <utils/array.h>
#include <utils/lsyscache.h>

#define  TYPALIGN_INT			'i'
#define  TYPALIGN_DOUBLE		'd'

extern void dgesdd_(char *JOBZ, int *m, int *n, double *A, int *lda, double *s, double *u, int *LDU, double *vt, int *l,
                    double *work, int *lwork, int *iwork, int *info);
extern void dgelsd( int* m, int* n, int* nrhs, double* a, int* lda,
                    double* b, int* ldb, double* s, double* rcond, int* rank,
                    double* work, int* lwork, int* iwork, int* info );

extern void dgemm (char *TRANSA, char* TRANSB, int *M, int* N, int *K, double *ALPHA,
                   double *A, int *LDA, double *B, int *LDB, double *BETA, double *C, int *LDC);

extern void dgemv (char *TRANSA, int *M, int* N, double *ALPHA,
                   double *A, int *LDA, double *X, int *INCX, double *BETA, double *Y, int *INCY);

int compare( const void* a, const void* b)
{
    int int_a = * ( (int*) a );
    int int_b = * ( (int*) b );

    if ( int_a == int_b ) return 0;
    else if ( int_a < int_b ) return -1;
    else return 1;
}

// #include <postgres.h>
// #include <utils/memutils.h>
// #include <math.h>

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
    double shrinkage = PG_GETARG_FLOAT8(2);

    size_t num_params = sizeof_sigma_matrix(cofactor, label);
    float8 *sigma_matrix = (float8 *)palloc0(sizeof(float8) * num_params * num_params);
    //elog(WARNING, " A ");
    //count distinct classes in var
    size_t num_categories = 0;
    const char *relation_data = crelation_array(cofactor);
    //elog(WARNING, " B ");
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
    uint64_t *cat_array = NULL;
    uint32_t *cat_vars_idxs = NULL;
    //todo tot columns does not support skip attributes, this requires sizeof_sigma_matrix
    size_t tot_columns = n_cols_1hot_expansion(&cofactor, 1, &cat_vars_idxs, &cat_array, 1, 0);//tot columns include label as well, so not used here
    double *sum_vector = (double *)palloc0(sizeof(double) * num_params * num_categories);
    double *mean_vector = (double *)palloc0(sizeof(double) * num_params * num_categories);
    double *coef = (double *)palloc0(sizeof(double) * num_params * num_categories);//from mean to coeff

    //uint64_t *idx_classes = (uint64_t *)palloc0(sizeof(uint64_t) * num_categories);//order of classes
    //elog(WARNING, " D ");
    build_sigma_matrix(cofactor, num_params, label, cat_array, cat_vars_idxs, 0, sigma_matrix);
    //elog(WARNING, " E ");
    build_sum_matrix(cofactor, num_params, label, cat_array, cat_vars_idxs, 0, sum_vector);

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
    //double shrinkage = 0.4;
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
    int num_params_int = (int) num_params;
    int num_categories_int = (int) num_categories;

    //elog(WARNING, " SOLVING ");
    lwork = -1;
    dgelsd( &num_params_int, &num_params_int, &num_categories_int, sigma_matrix, &num_params_int, coef, &num_params_int, s, &rcond, &rank, &wkopt, &lwork, iwork, &err);
    lwork = (int)wkopt;
    work = (double*)malloc( lwork*sizeof(double) );
    dgelsd( &num_params_int, &num_params_int, &num_categories_int, sigma_matrix, &num_params_int, coef, &num_params_int, s, &rcond, &rank, work, &lwork, iwork, &err);
    //elog(WARNING, "finished with err: %d", err);

    //compute intercept

    double alpha = 1;
    double beta = 0;
    double *res = (double *)palloc(num_categories*num_categories*sizeof(double));

    char task = 'N';
    dgemm(&task, &task, &num_categories_int, &num_categories_int, &num_params_int, &alpha, mean_vector, &num_categories_int, coef, &num_params_int, &beta, res, &num_categories_int);
    //elog(WARNING, "end!");
    double *intercept = (double *)palloc(num_categories*sizeof(double));
    for (size_t j = 0; j < num_categories; j++) {
        intercept[j] = (res[(j*num_categories)+j] * (-0.5)) + log(sum_vector[j * num_params] / cofactor->count);
    }

    // export in pgpsql. Return values
    Datum *d = (Datum *)palloc(sizeof(Datum) * (num_categories + 2 + (num_params * num_categories)));

    d[0] = Float4GetDatum((float)num_params);
    d[1] = Float4GetDatum((float)num_categories);

    //returns classes order
    /*
    for (int i = 0; i < num_categories; i++) {
        d[i + 2] = Float4GetDatum((float) idx_classes[i]);
    }*/

    //return coefficients
    for (int i = 0; i < num_params * num_categories; i++) {
        d[i + 2] = Float4GetDatum((float) coef[i]);
        //elog(WARNING, "coeff %lf", coef[i]);
    }

    //return intercept
    for (int i = 0; i < num_categories; i++) {
        d[i + 2 + (num_params * num_categories)] = Float4GetDatum((float) intercept[i]);
        //elog(WARNING, "intercept %lf", intercept[i]);
    }

    ArrayType *a = construct_array(d, (num_categories + 2 + (num_params * num_categories)), FLOAT4OID, sizeof(float4), true, TYPALIGN_INT);
    PG_RETURN_ARRAYTYPE_P(a);
}

PG_FUNCTION_INFO_V1(lda_impute);
Datum lda_impute(PG_FUNCTION_ARGS)
{
    //elog(WARNING, "Predicting...");
    //make sure encoding (order) of features is the same of cofactor and input data
    ArrayType *means_covariance = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *feats_numerical = PG_GETARG_ARRAYTYPE_P(1);
    ArrayType *feats_categorical = PG_GETARG_ARRAYTYPE_P(2);
    //ArrayType *max_feats_categorical = PG_GETARG_ARRAYTYPE_P(3);

    Oid arrayElementType1 = ARR_ELEMTYPE(means_covariance);
    Oid arrayElementType2 = ARR_ELEMTYPE(feats_numerical);
    Oid arrayElementType3 = ARR_ELEMTYPE(feats_categorical);
    //Oid arrayElementType4 = ARR_ELEMTYPE(max_feats_categorical);

    int16 arrayElementTypeWidth1, arrayElementTypeWidth2, arrayElementTypeWidth3, arrayElementTypeWidth4;
    bool arrayElementTypeByValue1, arrayElementTypeByValue2, arrayElementTypeByValue3, arrayElementTypeByValue4;
    Datum *arrayContent1, *arrayContent2, *arrayContent3, *arrayContent4;
    bool *arrayNullFlags1, *arrayNullFlags2, *arrayNullFlags3, *arrayNullFlags4;
    int arrayLength1, arrayLength2, arrayLength3, arrayLength4;
    char arrayElementTypeAlignmentCode1, arrayElementTypeAlignmentCode2, arrayElementTypeAlignmentCode3, arrayElementTypeAlignmentCode4;

    get_typlenbyvalalign(arrayElementType1, &arrayElementTypeWidth1, &arrayElementTypeByValue1, &arrayElementTypeAlignmentCode1);
    deconstruct_array(means_covariance, arrayElementType1, arrayElementTypeWidth1, arrayElementTypeByValue1, arrayElementTypeAlignmentCode1,
                      &arrayContent1, &arrayNullFlags1, &arrayLength1);

    get_typlenbyvalalign(arrayElementType2, &arrayElementTypeWidth2, &arrayElementTypeByValue2, &arrayElementTypeAlignmentCode2);
    deconstruct_array(feats_numerical, arrayElementType2, arrayElementTypeWidth2, arrayElementTypeByValue2, arrayElementTypeAlignmentCode2,
                      &arrayContent2, &arrayNullFlags2, &arrayLength2);

    get_typlenbyvalalign(arrayElementType3, &arrayElementTypeWidth3, &arrayElementTypeByValue3, &arrayElementTypeAlignmentCode3);
    deconstruct_array(feats_categorical, arrayElementType3, arrayElementTypeWidth3, arrayElementTypeByValue3, arrayElementTypeAlignmentCode3,
                      &arrayContent3, &arrayNullFlags3, &arrayLength3);
    /*
    get_typlenbyvalalign(arrayElementType4, &arrayElementTypeWidth4, &arrayElementTypeByValue4, &arrayElementTypeAlignmentCode4);
    deconstruct_array(max_feats_categorical, arrayElementType4, arrayElementTypeWidth4, arrayElementTypeByValue4, arrayElementTypeAlignmentCode4,
                      &arrayContent4, &arrayNullFlags4, &arrayLength4);
*/

    int num_params = (int) DatumGetFloat4(arrayContent1[0]);
    int num_categories = (int) DatumGetFloat4(arrayContent1[1]);
    int size_one_hot = num_params - arrayLength2 - 1;//PG_GETARG_INT64(3);

    //elog(WARNING, " size_one_hot : %d ", size_one_hot);
    //elog(WARNING, " size_numerical : %d ", arrayLength2);


    //int *idx_classes = (int *)palloc0(sizeof(int) * num_categories);//classes order
    double *coefficients = (double *)palloc0(sizeof(double) * num_params * num_categories);
    float *intercept = (float *)palloc0(sizeof(float) * num_categories);

    for(int i=0;i< num_categories;i++)
        for(int j=0;j< num_params;j++)
            coefficients[(j*num_categories)+i] = (double) DatumGetFloat4(arrayContent1[(i*num_params)+j+2]);

    for(int i=0;i<num_categories;i++)
        intercept[i] = (double) DatumGetFloat4(arrayContent1[i+(num_params * num_categories)+2]);

    //allocate features

    double *feats_c = (double *)palloc0(sizeof(double) * (size_one_hot + arrayLength2 + 1));
    feats_c[0] = 1;

    for(int i=0;i<arrayLength2;i++) {
        //elog(WARNING, " numerical: %lf ", DatumGetFloat4(arrayContent2[i]));
        feats_c[1 + i] = (double) DatumGetFloat4(arrayContent2[i]);
    }

    int cat_padding = 0;
    for(int i=0;i<arrayLength3;i++) {
        feats_c[1 + arrayLength2 + DatumGetInt64(arrayContent3[i])] = 1;//cat_padding
        //elog(WARNING, " categorical: %d ", DatumGetInt64(arrayContent3[i]));
        //elog(WARNING, " categorical + padding: %d ", DatumGetInt64(cat_padding + DatumGetInt64(arrayContent3[i])));
        //cat_padding += DatumGetInt64(arrayContent4[i]);
    }

    /*for(int i=0;i<size_one_hot + arrayLength2 + 1;i++) {
        elog(WARNING, " Feats: %lf ", feats_c[i]);
    }*/

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
        if (val > max){
            max = val;
            class = i;
        }
    }
    //elog(WARNING, "Prediction: %d", class);

    PG_RETURN_INT64(class);
}
