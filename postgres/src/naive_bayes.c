#include "nb_aggregates.h"
#include <postgres.h>
#include <fmgr.h>
#include <catalog/pg_type.h>
#include <utils/array.h>
#include <math.h>
#include <assert.h>

#include <utils/array.h>
#include <utils/lsyscache.h>
#include "relation.h"

#define _USE_MATH_DEFINES
#define  TYPALIGN_INT			'i'
#define  TYPALIGN_DOUBLE		'd'


PG_FUNCTION_INFO_V1(naive_bayes_train);
Datum naive_bayes_train(PG_FUNCTION_ARGS)
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

    for(size_t i=0; i<n_aggregates; i++) {
        aggregates[i] = (cofactor_t *) DatumGetPointer(arrayContent1[i]);
    }

    uint64_t *cat_array = NULL; //(uint64_t *)palloc0(sizeof(uint64_t) * num_categories);//max. size
    uint32_t *cat_vars_idxs = NULL; //(uint32_t *)palloc0(sizeof(uint32_t) * (aggregates[0]->num_categorical_vars + 1)); // track start each cat. variable

    float total_tuples = 0;
    for(size_t k=0; k<n_aggregates; k++) {
        total_tuples += aggregates[k]->count;
    }

    size_t total_cols = n_cols_1hot_expansion(aggregates, n_aggregates, &cat_vars_idxs, &cat_array, 0, 0);

    //compute mean and variance for every numerical feature
    //result = n. aggregates (classes), n. cat. values (cat_array size), cat_array, probs for each class, mean, variance for every num. feat. in 1st aggregate, prob. each cat. value 1st aggregate, ...
    Datum *result = (Datum *)palloc0(sizeof(Datum) * ((2*aggregates[0]->num_continuous_vars*n_aggregates)//continuous
                                        +(cat_vars_idxs[aggregates[0]->num_categorical_vars]*n_aggregates)//categoricals
                                        +aggregates[0]->num_categorical_vars + 1 //cat. vars. idxs
                                        +n_aggregates + 1 + 1 + cat_vars_idxs[aggregates[0]->num_categorical_vars]));
    result[0] = Float4GetDatum(n_aggregates);
    result[1] = Float4GetDatum(aggregates[0]->num_categorical_vars+1);

    for(size_t i=0; i<aggregates[0]->num_categorical_vars + 1; i++)
        result[i+2] = Float4GetDatum(cat_vars_idxs[i]);

    for(size_t i=0; i<cat_vars_idxs[aggregates[0]->num_categorical_vars]; i++)
        result[i+2+aggregates[0]->num_categorical_vars+1] = Float4GetDatum(cat_array[i]);

    size_t k=n_aggregates+2+cat_vars_idxs[aggregates[0]->num_categorical_vars]+aggregates[0]->num_categorical_vars+1;
    for(size_t i=0; i<n_aggregates; i++) {
        result[i+2+cat_vars_idxs[aggregates[0]->num_categorical_vars]+aggregates[0]->num_categorical_vars+1] = Float4GetDatum(aggregates[i]->count / total_tuples);
        for (size_t j=0; j<aggregates[i]->num_continuous_vars; j++){
            float mean = (float)scalar_array(aggregates[i])[j] / (float)aggregates[i]->count;
            float variance = ((float)scalar_array(aggregates[i])[j + aggregates[i]->num_continuous_vars] / (float)aggregates[i]->count) - (mean * mean);
            result[k] = Float4GetDatum(mean);
            result[k+1] = Float4GetDatum(variance);
            k+=2;
        }
        k += cat_vars_idxs[aggregates[0]->num_categorical_vars];//categorical will be set later
    }

    k=n_aggregates+2+cat_vars_idxs[aggregates[0]->num_categorical_vars]+(aggregates[0]->num_continuous_vars*2)+aggregates[0]->num_categorical_vars + 1;
    for(size_t i=0; i<n_aggregates; i++) {
        const char *relation_data = crelation_array(aggregates[i]);
        for (size_t j=0; j<aggregates[i]->num_categorical_vars; j++){
            relation_t *r = (relation_t *) relation_data;
            for (size_t l = 0; l < r->num_tuples; l++) {
                size_t index = find_in_array(r->tuples[l].key, cat_array, cat_vars_idxs[j], cat_vars_idxs[j+1]);
                //elog(WARNING, "find value %zu from %zu to %zu: index: %zu", r->tuples[l].key, cat_vars_idxs[j], cat_vars_idxs[j+1], index);
                result[index + k] = Float4GetDatum((float) r->tuples[l].value / (float) aggregates[i]->count);
            }
            relation_data += r->sz_struct;
        }
        k += cat_vars_idxs[aggregates[i]->num_categorical_vars] + (aggregates[i]->num_continuous_vars*2);
    }

    ArrayType *a = construct_array(result, ((2*aggregates[0]->num_continuous_vars*n_aggregates)+(cat_vars_idxs[aggregates[0]->num_categorical_vars]*n_aggregates)+n_aggregates+1+ 1 + cat_vars_idxs[aggregates[0]->num_categorical_vars] + aggregates[0]->num_categorical_vars + 1), FLOAT4OID, sizeof(float4), true, TYPALIGN_INT);
    PG_RETURN_ARRAYTYPE_P(a);
}

PG_FUNCTION_INFO_V1(naive_bayes_predict);
Datum naive_bayes_predict(PG_FUNCTION_ARGS) {
    ArrayType *parameters = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *features_cont = PG_GETARG_ARRAYTYPE_P(1);
    ArrayType *features_cat = PG_GETARG_ARRAYTYPE_P(2);

    Oid arrayElementType1 = ARR_ELEMTYPE(parameters);
    Oid arrayElementType2 = ARR_ELEMTYPE(features_cont);
    Oid arrayElementType3 = ARR_ELEMTYPE(features_cat);

    int16 arrayElementTypeWidth1, arrayElementTypeWidth2, arrayElementTypeWidth3;
    bool arrayElementTypeByValue1, arrayElementTypeByValue2, arrayElementTypeByValue3;
    Datum *arrayContent1, *arrayContent2, *arrayContent3;
    bool *arrayNullFlags1, *arrayNullFlags2, *arrayNullFlags3;
    int n_params, n_feats_cont, n_feats_cat;
    char arrayElementTypeAlignmentCode1, arrayElementTypeAlignmentCode2, arrayElementTypeAlignmentCode3;

    get_typlenbyvalalign(arrayElementType1, &arrayElementTypeWidth1, &arrayElementTypeByValue1, &arrayElementTypeAlignmentCode1);
    deconstruct_array(parameters, arrayElementType1, arrayElementTypeWidth1, arrayElementTypeByValue1, arrayElementTypeAlignmentCode1,
                      &arrayContent1, &arrayNullFlags1, &n_params);
    get_typlenbyvalalign(arrayElementType2, &arrayElementTypeWidth2, &arrayElementTypeByValue2, &arrayElementTypeAlignmentCode2);
    deconstruct_array(features_cont, arrayElementType2, arrayElementTypeWidth2, arrayElementTypeByValue2, arrayElementTypeAlignmentCode2,
                      &arrayContent2, &arrayNullFlags2, &n_feats_cont);
    get_typlenbyvalalign(arrayElementType3, &arrayElementTypeWidth3, &arrayElementTypeByValue3, &arrayElementTypeAlignmentCode3);
    deconstruct_array(features_cat, arrayElementType3, arrayElementTypeWidth3, arrayElementTypeByValue3, arrayElementTypeAlignmentCode3,
                      &arrayContent3, &arrayNullFlags3, &n_feats_cat);

    int n_classes = DatumGetFloat4(arrayContent1[0]);
    int size_idxs = DatumGetFloat4(arrayContent1[1]);
    size_t k=2+n_classes;//2+priors
    size_t prior_offset = 2;
    uint64_t *cat_vars_idxs;
    uint64_t *cat_vars;

    if(size_idxs > 0) {
        cat_vars_idxs = (uint64_t *) palloc0(sizeof(uint64_t) * (size_idxs));//max. size
        for (size_t i = 0; i < size_idxs; i++)
            cat_vars_idxs[i] = DatumGetFloat4(arrayContent1[i + 2]);

        cat_vars = (uint64_t *) palloc(sizeof(uint64_t) * cat_vars_idxs[size_idxs - 1]);//max. size
        for (size_t i = 0; i < cat_vars_idxs[size_idxs - 1]; i++)
            cat_vars[i] = DatumGetFloat4(arrayContent1[i + 2 + size_idxs]);

        for (size_t i = 0; i < size_idxs; i++) {
            elog(WARNING, "n_feats_cat_idxs %zu", cat_vars_idxs[i]);
        }
        for (size_t i = 0; i < cat_vars_idxs[size_idxs - 1]; i++) {
            elog(WARNING, "cat. feats. %zu", cat_vars[i]);
        }

        k=2+cat_vars_idxs[size_idxs-1]+size_idxs+n_classes;
        prior_offset = 2+cat_vars_idxs[size_idxs-1]+size_idxs;
    }

    int best_class = 0;
    double max_prob = 0;
    for(size_t i=0; i<n_classes; i++){
        double total_prob = DatumGetFloat4(arrayContent1[prior_offset+i]);
        elog(WARNING, "total prob %f", total_prob);

        for(size_t j=0; j<n_feats_cont; j++){
            double variance = DatumGetFloat4(arrayContent1[k+(j*2)+1]);
            variance += 0.000000001;//avoid division by 0
            double mean = DatumGetFloat4(arrayContent1[k+(j*2)]);
            total_prob *= ((double)1 / sqrt(2*M_PI*variance)) * exp( -(pow((DatumGetFloat4(arrayContent2[j]) - mean), 2)) / ((double)2*variance));
            elog(WARNING, "total prob %f (normal mean %lf var %lf)", total_prob, mean, variance);
        }

        k += (2*n_feats_cont);
        if (size_idxs > 0) {//if categorical features
            for (size_t j = 0; j < n_feats_cat; j++) {
                uint64_t class = DatumGetInt64(arrayContent3[j]);
                size_t index = find_in_array(class, cat_vars, cat_vars_idxs[j], cat_vars_idxs[j + 1]);
                elog(WARNING, "class %zu index %zu", class, index);
                if (index == cat_vars_idxs[j + 1])//class not found in train dataset
                    total_prob *= 0;
                else {
                    total_prob *= DatumGetFloat4(
                            arrayContent1[k + index]);//cat. feats need to be monot. increasing
                    elog(WARNING, "multiply %lf", DatumGetFloat4(arrayContent1[k + index]));
                    elog(WARNING, "total prob %lf", total_prob);
                }
            }
            k += cat_vars_idxs[n_feats_cat];
        }
        elog(WARNING, "final prob for class %d %lf", i, total_prob);

        if (total_prob > max_prob){
            max_prob = total_prob;
            best_class = i;
        }
    }
    PG_RETURN_INT64(best_class);
}