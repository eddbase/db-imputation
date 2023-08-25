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

static size_t find_in_array(uint64_t a, const uint64_t *array, size_t start, size_t end)
{
    size_t index = start;
    while (index < end)
    {
        if (array[index] == a)
            break;
        index++;
    }
    return index;
}

static size_t get_num_categories(const cofactor_t *cofactor, int label_categorical_sigma)
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
}


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

    size_t num_categories = 0;
    for(size_t k=0; k<n_aggregates; k++) {
        num_categories += get_num_categories(aggregates[k], -1);
    }

    uint64_t *cat_array = (uint64_t *)palloc0(sizeof(uint64_t) * num_categories);//max. size
    uint32_t *cat_vars_idxs = (uint32_t *)palloc0(sizeof(uint32_t) * aggregates[0]->num_categorical_vars + 1); // track start each cat. variable
    char **relation_scan = (char **)palloc0(sizeof(char *) * n_aggregates); // track start each cat. variable

    float total_tuples = 0;
    for(size_t k=0; k<n_aggregates; k++) {
        relation_scan[k] = relation_array(aggregates[k]);
        total_tuples += aggregates[k]->count;
    }

    cat_vars_idxs[0] = 0;
    size_t search_start = 0;        // within one category class
    size_t search_end = search_start;
    //detect number of categorical values in every feature and sort them
    for (size_t i = 0; i < aggregates[0]->num_categorical_vars; i++) {
        for(size_t k=0; k<n_aggregates; k++) {
            relation_t *r = (relation_t *) relation_scan[k];
            //elog(WARNING, "aggregate %zu num tuples: %zu", k, r->num_tuples);
            //create sorted array
            for (size_t j = 0; j < r->num_tuples; j++) {
                elog(WARNING, "find %zu start %zu end %zu", k, search_start, search_end);
                size_t key_index = find_in_array(r->tuples[j].key, cat_array, search_start, search_end);
                if (key_index == search_end) {
                    //elog(WARNING, "new value, key = %zu", r->tuples[j].key);
                    uint64_t value_to_insert = r->tuples[j].key;
                    uint64_t tmp;
                    for (size_t k = search_start; k < search_end; k++) {
                        if (value_to_insert < cat_array[k]) {
                            tmp = cat_array[k];
                            cat_array[k] = value_to_insert;
                            value_to_insert = tmp;
                        }
                    }
                    cat_array[search_end] = value_to_insert;
                    search_end++;
                }
            }
            relation_scan[k] += r->sz_struct;
        }
        cat_vars_idxs[i + 1] = cat_vars_idxs[i] + (search_end - search_start);
        search_start = search_end;
        //elog(WARNING, "cat_var %zu", cat_vars_idxs[i+1]);
    }

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
    uint64_t *cat_vars_idxs = (uint64_t *)palloc0(sizeof(uint64_t) * (size_idxs));//max. size
    for(size_t i=0; i<size_idxs; i++)
        cat_vars_idxs[i] = DatumGetFloat4(arrayContent1[i+2]);

    uint64_t *cat_vars = (uint64_t *)palloc(sizeof(uint64_t) * cat_vars_idxs[size_idxs-1]);//max. size
    for (size_t i=0; i<cat_vars_idxs[size_idxs-1];i++)
        cat_vars[i] = DatumGetFloat4(arrayContent1[i+2+size_idxs]);

    for(size_t i=0; i<size_idxs; i++){
        elog(WARNING, "n_feats_cat_idxs %zu", cat_vars_idxs[i]);
    }
    for(size_t i=0; i<cat_vars_idxs[size_idxs-1]; i++){
        elog(WARNING, "cat. feats. %zu", cat_vars[i]);
    }

    int best_class = 0;
    double max_prob = 0;
    size_t k=2+cat_vars_idxs[size_idxs-1]+size_idxs+n_classes;

    for(size_t i=0; i<n_classes; i++){
        double total_prob = DatumGetFloat4(arrayContent1[2+cat_vars_idxs[size_idxs-1]+size_idxs+i]);
        elog(WARNING, "total prob %f", total_prob);

        for(size_t j=0; j<n_feats_cont; j++){
            double variance = DatumGetFloat4(arrayContent1[k+(j*2)+1]);
            variance += 0.00000001;//avoid division by 0
            double mean = DatumGetFloat4(arrayContent1[k+(j*2)]);
            total_prob *= ((double)1 / sqrt(2*M_PI*variance)) * exp( -(pow((DatumGetFloat4(arrayContent2[j]) - mean), 2)) / ((double)2*variance));
            elog(WARNING, "total prob %f (normal mean %lf var %lf)", total_prob, mean, variance);
        }

        k += (2*n_feats_cont);
        for(size_t j=0; j<n_feats_cat; j++){
            uint64_t class = DatumGetInt64(arrayContent3[j]);
            size_t index = find_in_array(class, cat_vars, cat_vars_idxs[j], cat_vars_idxs[j+1]);
            elog(WARNING, "class %zu index %zu", class, index);
            if (index == cat_vars_idxs[j+1])//class not found in train dataset
                total_prob *= 0;
            else {
                total_prob *= DatumGetFloat4(arrayContent1[k + index]);//cat. feats need to be monot. increasing
                elog(WARNING, "multiply %lf", DatumGetFloat4(arrayContent1[k + index]));
                elog(WARNING, "total prob %lf", total_prob);
            }
        }
        k += cat_vars_idxs[n_feats_cat];

        if (total_prob > max_prob){
            max_prob = total_prob;
            best_class = i;
        }
    }
    PG_RETURN_INT64(best_class);
}