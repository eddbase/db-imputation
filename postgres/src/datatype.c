#include <datatype.h>
#include <relation.h>

static inline size_t size_scalar_array(size_t num_cont, int is_cofactor)
{
    // num_cont * (num_cont + 3) / 2
    if (is_cofactor){
        //elog(WARNING, "IS COFACTOR ");
        return (num_cont *(num_cont+ 3)) >> 1;
    }
    return num_cont * 2;
}

static inline char *relation_array(cofactor_t *c, int is_cofactor)
{
    return (char *)(scalar_array(c) + size_scalar_array(c->num_continuous_vars, is_cofactor));
}

static inline const char *crelation_array(const cofactor_t *c, int is_cofactor)
{
    return (const char *)(cscalar_array(c) + size_scalar_array(c->num_continuous_vars, is_cofactor));
}

size_t n_cols_1hot_expansion(const cofactor_t **cofactors, size_t n_aggregates, uint32_t **cat_idxs, uint64_t **cat_unique_array, int is_cofactor, int drop_first)
{
    size_t num_categories = 0;
    for(size_t k=0; k<n_aggregates; k++) {
        num_categories += get_num_categories(crelation_array(cofactors[k], is_cofactor), cofactors[k]->num_categorical_vars, -1);//potentially overestimate
    }
    //elog(WARNING, "1 %d %d", cofactors[0]->num_categorical_vars + 1, num_categories);
    uint32_t *cat_vars_idxs = (uint32_t *)palloc0(sizeof(uint32_t) * (cofactors[0]->num_categorical_vars + 1)); // track start each cat. variable
    uint64_t *cat_array = (uint64_t *)palloc0(sizeof(uint64_t) * num_categories);//max. size

    (*cat_idxs) = cat_vars_idxs;
    (*cat_unique_array) = cat_array;
    //elog(WARNING, "2");

    cat_vars_idxs[0] = 0;
    size_t search_start = 0;        // within one category class
    size_t search_end = search_start;
    //elog(WARNING, "3");

    char **relation_scan = (char **)palloc0(sizeof(char *) * n_aggregates); // track start each cat. variable
    for(size_t k=0; k<n_aggregates; k++) {
        relation_scan[k] = relation_array(cofactors[k], is_cofactor);
    }
    //elog(WARNING, "4");

    for (size_t i = 0; i < cofactors[0]->num_categorical_vars; i++) {
        for(size_t k=0; k<n_aggregates; k++) {
            relation_t *r = (relation_t *) relation_scan[k];
            //elog(WARNING, "5");
            //create sorted array
            for (size_t j = 0; j < r->num_tuples; j++) {
                size_t key_index = find_in_array(r->tuples[j].key, cat_array, search_start, search_end);
                if (key_index == search_end) {
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
    }
    //elog(WARNING, "6");

    if (drop_first){//remove first entry for each categorical attribute (avoids multicollinearity)
        for (size_t i = 0; i < cofactors[0]->num_categorical_vars; i++){
            cat_vars_idxs[i+1]-= (i+1);
            for(size_t j=cat_vars_idxs[i]; j<cat_vars_idxs[i+1]; j++){
                cat_array[j] = cat_array[j+i+1];
            }
        }
    }

    // count :: numerical :: 1-hot_categories
    return 1 + cofactors[0]->num_continuous_vars + cat_vars_idxs[cofactors[0]->num_categorical_vars];
}

// number of categories in relations formed for group by A, group by B, ...
// assumption: relations contain distinct tuples
size_t get_num_categories(const char *relation_data_o, size_t num_categorical_vars, int label_categorical_sigma)
{
    size_t num_categories = 0;
    const char *relation_data = relation_data_o;
    for (size_t i = 0; i < num_categorical_vars; i++)
    {
        //elog(WARNING, "NUM_CAT_VARS %d", i);
        relation_t *r = (relation_t *) relation_data;

        if (label_categorical_sigma >= 0 && ((size_t)label_categorical_sigma) == i)
        {
            //skip this variable
            relation_data += r->sz_struct;
            continue;
        }
        //elog(WARNING, "num_tuples %lu", r->num_tuples);
        num_categories += r->num_tuples;
        relation_data += r->sz_struct;
    }
    return num_categories;
}

size_t find_in_array(uint64_t a, const uint64_t *array, size_t start, size_t end)
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