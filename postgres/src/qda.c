#include "cofactor.h"
#include "relation.h"
#include <postgres.h>
#include <fmgr.h>
#include <catalog/pg_type.h>
#include <utils/array.h>
#include <math.h>
#include <assert.h>
#include <float.h>

#include <utils/array.h>
#include <utils/lsyscache.h>

#define  TYPALIGN_INT			'i'
#define  TYPALIGN_DOUBLE		'd'

extern void dgemm (char *TRANSA, char* TRANSB, int *M, int* N, int *K, double *ALPHA,
                   double *A, int *LDA, double *B, int *LDB, double *BETA, double *C, int *LDC);

extern void dgemv (char *TRANSA, int *M, int* N, double *ALPHA,
                   double *A, int *LDA, double *X, int *INCX, double *BETA, double *Y, int *INCY);

extern void dgesvd( char* jobu, char* jobvt, int* m, int* n, double* a,
                    int* lda, double* s, double* u, int* ldu, double* vt, int* ldvt,
                    double* work, int* lwork, int* info );

extern void dscal( int* N, double* DA, double *DX, int* INCX );

PG_FUNCTION_INFO_V1(qda_train);

Datum qda_train(PG_FUNCTION_ARGS)
{
    ArrayType *cofactors = PG_GETARG_ARRAYTYPE_P(0);
    Oid arrayElementType1 = ARR_ELEMTYPE(cofactors);
    int16 arrayElementTypeWidth1;
    bool arrayElementTypeByValue1;
    Datum *arrayContent1;
    bool *arrayNullFlags1;
    int n_aggregates;
    char arrayElementTypeAlignmentCode1;

    get_typlenbyvalalign(arrayElementType1, &arrayElementTypeWidth1, &arrayElementTypeByValue1, &arrayElementTypeAlignmentCode1);
    deconstruct_array(cofactors, arrayElementType1, arrayElementTypeWidth1, arrayElementTypeByValue1, arrayElementTypeAlignmentCode1,
                      &arrayContent1, &arrayNullFlags1, &n_aggregates);

    cofactor_t **aggregates = (cofactor_t **)palloc0(sizeof(cofactor_t *) * n_aggregates);
    elog(WARNING, "a");
    for(size_t i=0; i<n_aggregates; i++) {
        aggregates[i] = (cofactor_t *) DatumGetPointer(arrayContent1[i]);
    }

    double shrinkage = PG_GETARG_FLOAT8(1);

    uint32_t *cat_idxs = NULL;
    uint64_t *cat_array = NULL;
    elog(WARNING, "b");
    int drop_first = 1;
    size_t num_params = n_cols_1hot_expansion(aggregates, n_aggregates, &cat_idxs, &cat_array, 1, drop_first);//enable drop first
    size_t num_categories = cat_idxs[aggregates[0]->num_categorical_vars];
    for(size_t i=0; i<aggregates[0]->num_categorical_vars+1; i++){
        elog(WARNING, "cat_idxs %d", cat_idxs[i]);
    }
    for(size_t i=0; i<cat_idxs[aggregates[0]->num_categorical_vars]; i++){
        elog(WARNING, "cat_array %d", cat_array[i]);
    }

    float8 *sigma_matrix = (float8 *)palloc0(sizeof(float8) * num_params * num_params);
    double *sum_vector = (double *)palloc0(sizeof(double) * num_params);
    //double *coef = (double *)palloc0(sizeof(double) * num_params * num_categories);//from mean to coeff

    int m = num_params-1;
    double *sing_values = (double*)palloc(m*sizeof(double));
    double *u = (double*)palloc(m*m*sizeof(double));
    double *vt = (double*) palloc(m*m*sizeof(double));
    double* inva = (double*)palloc(m*m*sizeof(double));
    double* mean_vector = (double*)palloc0(sizeof(double) * (m));//remove constant term
    double *lin_result = (double *)palloc0(sizeof(double) * m);

    size_t res_size = (1+1+ (((m*m)+m+1)*n_aggregates));
    if (aggregates[0]->num_categorical_vars > 0){
        res_size += aggregates[0]->num_categorical_vars+1 + cat_idxs[aggregates[0]->num_categorical_vars];
    }
    Datum *result = (Datum *)palloc(sizeof(Datum) * res_size);
    result[0] = Float4GetDatum(n_aggregates);

    size_t param_out_index = 2;

    if (aggregates[0]->num_categorical_vars > 0) {
        result[1] = Float4GetDatum(aggregates[0]->num_categorical_vars + 1);
        for(size_t i=0; i<aggregates[0]->num_categorical_vars+1; i++)
            result[i+2] = Float4GetDatum(cat_idxs[i]);

        param_out_index = aggregates[0]->num_categorical_vars+3;
        for(size_t i=0; i<cat_idxs[aggregates[0]->num_categorical_vars]; i++)
            result[param_out_index+i] = Float4GetDatum(cat_array[i]);
        param_out_index += cat_idxs[aggregates[0]->num_categorical_vars];
    }
    else
        result[1] = Float4GetDatum(0);

    elog(WARNING, "d");
    double tot_tuples = 0;
    for(size_t i=0; i<n_aggregates; i++){
        tot_tuples += aggregates[i]->count;
    }
    elog(WARNING, "e");

    for(size_t i=0; i<n_aggregates; i++) {
        elog(WARNING, "starting sigma");

        for (size_t j = 0; j < num_params; j++) {
            sum_vector[j] = 0;
            for (size_t k = 0; k < num_params; k++)
                sigma_matrix[((j) * (num_params)) + (k)] = 0;
        }

        build_sigma_matrix(aggregates[i], num_params, -1, cat_array, cat_idxs, drop_first, sigma_matrix);
        elog(WARNING, "done sigma");
        build_sum_vector(aggregates[i], num_params, cat_array, cat_idxs, drop_first, sum_vector);
        elog(WARNING, "done sum");

        for (size_t j = 1; j < num_params; j++) {
                elog(WARNING, "sum vector: %lf", sum_vector[j]);
        }

                //generate covariance matrix
        for (size_t j = 1; j < num_params; j++) {
            for (size_t k = 1; k < num_params; k++) {
                elog(WARNING, "sigma matrix row: %d col: %d val: %lf", j-1, k-1, sigma_matrix[((j) * (num_params)) + (k)]);
                sigma_matrix[((j-1) * (num_params-1)) + (k-1)] = (sigma_matrix[(j * num_params) + k] - ((float8)(sum_vector[j] * sum_vector[k]) / (float8) sum_vector[0])) / (float8) sum_vector[0];
                //sigma_matrix[(j * num_params) + k] /= (float8) sum_vector[0];
                elog(WARNING, "covariance row: %d col: %d val: %lf", j-1, k-1, sigma_matrix[((j-1) * (num_params-1)) + (k-1)]);
            }
            mean_vector[j-1] = sum_vector[j]/sum_vector[0];
        }
        elog(WARNING, "f");

        //invert the matrix. We can also use LU decomposition which is faster but less stable
        int lwork = -1;
        double wkopt;
        int info;
        dgesvd( "S", "S", &m, &m, sigma_matrix, &m, sing_values, u, &m, vt, &m, &wkopt, &lwork, &info );
        double *work = (double*) palloc(((int) wkopt)*sizeof(double));
        lwork = wkopt;
        /* Compute SVD */
        dgesvd( "S", "S", &m, &m, sigma_matrix, &m, sing_values, u, &m, vt, &m, work, &lwork, &info );
        /* Check for convergence */
        if( info > 0 ) {
            elog(WARNING, "The algorithm computing SVD failed to converge." );
        }
        //we computed SVD, now calculate u=Σ-1U, where Σ is diagonal, so compute by a loop of calling BLAS ?scal function for computing product of a vector by a scalar
        //u is stored column-wise, and vt is row-wise here, thus the formula should be like u=UΣ-1
        double rcond = (1.0e-15);//Cutoff for small singular values. Singular values less than or equal to rcond * largest_singular_value are set to zero
        //todoc consider using syevd for SVD. For positive semi-definite matrices (like covariance) SVD and eigenvalue decomp. are the same
        /*for(int i=0; i<m; i++){
            if (sing_values[i] <= sing_values[0]*rcond)
                sing_values[i] = 0;
        }*/

        int incx = 1;
        for(int i=0; i<m; i++)
        {
            double ss;
            if(sing_values[i] > 1.0e-9)
                ss=1.0/sing_values[i];//1/sing. value is the inverse of diagonal matrix
            else
                ss=sing_values[i];
            dscal(&m, &ss, &u[i*m], &incx);
        }
        double determinant = 1;
        for(int i=0; i<m; i++)
            determinant *= sing_values[i];

        elog(WARNING, "determinant %lf", determinant);
        //calculate A+=(V*)TuT, use MKL ?GEMM function to calculate matrix multiplication
        double alpha=1.0, beta=0.0;
        dgemm( "T", "T", &m, &m, &m, &alpha, vt, &m, u, &m, &beta, inva, &m);
        elog(WARNING, "g");

        //I have the inverse
        for(int i=0; i<m; i++){
            for(int j=0; j<m; j++){
                result[param_out_index+(i*m)+j] = Float4GetDatum((float) -1* inva[(i*m)+j]/2);
            }
        }
        elog(WARNING, "h");
        param_out_index += (m*m);
        //compute product with mean
        char task = 'N';
        int increment = 1;

        dgemv(&task, &m, &m, &alpha, inva, &m, mean_vector, &increment, &beta, lin_result, &increment);
        for(int j=0; j<m; j++){
            elog(WARNING, "mean %lf", mean_vector[j]);
            result[param_out_index+j] = Float4GetDatum((float) lin_result[j]);
        }
        elog(WARNING, "i");

        param_out_index += m;
        int row=1;
        double intercept = 0;
        dgemv(&task, &row, &m, &alpha, mean_vector, &row, lin_result, &increment, &beta, &intercept, &increment);
        elog(WARNING, "intercept 1 %lf", (intercept));
        for(int i=0; i<m; i++)
            elog(WARNING, " %lf * %lf", mean_vector[i], lin_result[i]);
        intercept = ((-1)*intercept/(double)2) - (log(determinant)/(double)2) + log(sum_vector[0]/(double)tot_tuples);
        elog(WARNING, "intercept 1 %lf", (log(determinant)/(double)2));
        elog(WARNING, "intercept 1 %lf", log(sum_vector[0]/(double)tot_tuples));
        result[param_out_index] = Float4GetDatum((float) intercept);
        param_out_index++;
        elog(WARNING, "l");
    }

    ArrayType *a = construct_array(result, res_size, FLOAT4OID, sizeof(float4), true, TYPALIGN_INT);
    PG_RETURN_ARRAYTYPE_P(a);
}

PG_FUNCTION_INFO_V1(qda_predict);
Datum qda_predict(PG_FUNCTION_ARGS)
{
    //elog(WARNING, "Predicting...");
    //make sure encoding (order) of features is the same of cofactor and input data
    ArrayType *means_covariance = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *feats_numerical = PG_GETARG_ARRAYTYPE_P(1);
    ArrayType *feats_categorical = PG_GETARG_ARRAYTYPE_P(2);
    //ArrayType *max_feats_categorical = PG_GETARG_ARRAYTYPE_P(3);
    elog(WARNING, "a");
    Oid arrayElementType1 = ARR_ELEMTYPE(means_covariance);
    Oid arrayElementType2 = ARR_ELEMTYPE(feats_numerical);
    Oid arrayElementType3 = ARR_ELEMTYPE(feats_categorical);
    //Oid arrayElementType4 = ARR_ELEMTYPE(max_feats_categorical);

    int16 arrayElementTypeWidth1, arrayElementTypeWidth2, arrayElementTypeWidth3;
    bool arrayElementTypeByValue1, arrayElementTypeByValue2, arrayElementTypeByValue3;
    Datum *arrayContent1, *arrayContent2, *arrayContent3;
    bool *arrayNullFlags1, *arrayNullFlags2, *arrayNullFlags3;
    int arrayLength1, arrayLength2, arrayLength3;
    char arrayElementTypeAlignmentCode1, arrayElementTypeAlignmentCode2, arrayElementTypeAlignmentCode3;
    elog(WARNING, "b");

    get_typlenbyvalalign(arrayElementType1, &arrayElementTypeWidth1, &arrayElementTypeByValue1, &arrayElementTypeAlignmentCode1);
    deconstruct_array(means_covariance, arrayElementType1, arrayElementTypeWidth1, arrayElementTypeByValue1, arrayElementTypeAlignmentCode1,
                      &arrayContent1, &arrayNullFlags1, &arrayLength1);

    get_typlenbyvalalign(arrayElementType2, &arrayElementTypeWidth2, &arrayElementTypeByValue2, &arrayElementTypeAlignmentCode2);
    deconstruct_array(feats_numerical, arrayElementType2, arrayElementTypeWidth2, arrayElementTypeByValue2, arrayElementTypeAlignmentCode2,
                      &arrayContent2, &arrayNullFlags2, &arrayLength2);

    get_typlenbyvalalign(arrayElementType3, &arrayElementTypeWidth3, &arrayElementTypeByValue3, &arrayElementTypeAlignmentCode3);
    deconstruct_array(feats_categorical, arrayElementType3, arrayElementTypeWidth3, arrayElementTypeByValue3, arrayElementTypeAlignmentCode3,
                      &arrayContent3, &arrayNullFlags3, &arrayLength3);

    int n_classes = DatumGetFloat4(arrayContent1[0]);
    int size_idxs = DatumGetFloat4(arrayContent1[1]);
    int one_hot_size = 0;
    uint64_t *cat_vars_idxs;
    uint64_t *cat_vars;
    elog(WARNING, "c");
    size_t k=2;

    if (size_idxs > 0) {
        cat_vars_idxs = (uint64_t *) palloc0(sizeof(uint64_t) * (size_idxs));//max. size
        for (size_t i = 0; i < size_idxs; i++)
            cat_vars_idxs[i] = DatumGetFloat4(arrayContent1[i + 2]);

        one_hot_size = cat_vars_idxs[size_idxs - 1];
        cat_vars = (uint64_t *) palloc(sizeof(uint64_t) * one_hot_size);//max. size
        for (size_t i = 0; i < cat_vars_idxs[size_idxs - 1]; i++)
            cat_vars[i] = DatumGetFloat4(arrayContent1[i + 2 + size_idxs]);

        for (size_t i = 0; i < size_idxs; i++) {
            elog(WARNING, "n_feats_cat_idxs %zu", cat_vars_idxs[i]);
        }
        for (size_t i = 0; i < cat_vars_idxs[size_idxs - 1]; i++) {
            elog(WARNING, "cat. feats. %zu", cat_vars[i]);
        }
        k=2+cat_vars_idxs[size_idxs-1]+size_idxs;
    }

    elog(WARNING, "d");

    /////
    size_t n_params = one_hot_size + arrayLength2;
    double *features = (double *)palloc0(sizeof(double) * (n_params));
    double *quad_matrix = (double *)palloc0(sizeof(double) * (n_params*n_params));
    double *lin_matrix = (double *)palloc0(sizeof(double) * (n_params*n_params));
    double *res_matmul = (double *)palloc0(sizeof(double) * (n_params));
    elog(WARNING, "e");

    int m = n_params;

    //copy features
    for(int i=0; i<arrayLength2; i++)
        features[i] = DatumGetFloat4(arrayContent2[i]);

    for(int i=0; i<arrayLength3; i++){//categorical feats
        int class = DatumGetInt64(arrayContent3[i]);
        size_t index = find_in_array(class, cat_vars, cat_vars_idxs[i], cat_vars_idxs[i+1]);
        elog(WARNING, "1-hot class %d -> index %d (from %d to %d)", class, index, cat_vars_idxs[i], cat_vars_idxs[i+1]);
        if (index < cat_vars_idxs[i+1])
            features[index+arrayLength2] = 1;
    }

    for(int i=0; i<n_params; i++){
        elog(WARNING, "feats %lf", features[i]);
    }

    int best_class = 0;
    double max_prob = -DBL_MAX;
    for(size_t i=0; i<n_classes; i++){
        //copy features
        for(size_t j=0; j<n_params*n_params; j++){
            quad_matrix[j] = DatumGetFloat4(arrayContent1[j + k]);
        }
        k += (n_params*n_params);
        for(size_t j=0; j<n_params; j++){
            lin_matrix[j] = DatumGetFloat4(arrayContent1[j + k]);
        }
        k += (n_params);
        double intercept = DatumGetFloat4(arrayContent1[k]);
        k++;
        elog(WARNING, "g");

        char task = 'N';
        int increment = 1;
        double alpha=1.0, beta=0.0;

        dgemv(&task, &m, &m, &alpha, quad_matrix, &m, features, &increment, &beta, res_matmul, &increment);
        int row=1;
        double res_prob_1 = 0;
        dgemv(&task, &row, &m, &alpha, res_matmul, &m, features, &increment, &beta, &res_prob_1, &increment);

        double res_prob_2 = 0;
        dgemv(&task, &row, &m, &alpha, lin_matrix, &m, features, &increment, &beta, &res_prob_2, &increment);

        double total_prob = intercept + res_prob_1 + res_prob_2;
        elog(WARNING, "prob: %lf", total_prob);

        if (total_prob > max_prob){
            max_prob = total_prob;
            best_class = i;
        }
    }

    PG_RETURN_INT64(best_class);
}
