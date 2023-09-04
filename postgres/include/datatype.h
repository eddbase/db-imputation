#ifndef POSTGRES_DATATYPE_H
#define POSTGRES_DATATYPE_H

#include <postgres.h>
#include <assert.h>
#include <datatype.h>

typedef struct
{
    uint32_t sz_struct; /* varlena header */
    uint32_t sz_relation_data;
    uint16_t num_continuous_vars;
    uint16_t num_categorical_vars;
    int32_t count;
    char data[FLEXIBLE_ARRAY_MEMBER]; // scalar data + relation data
} cofactor_t;
static_assert(sizeof(cofactor_t) == 16, "size of cofactor_t not 16 bytes");


inline float8 *scalar_array(cofactor_t *c)
{
    return (float8 *)c->data;
}

inline const float8 *cscalar_array(const cofactor_t *c)
{
    return (const float8 *)c->data;
}

size_t get_num_categories(const char *relation_data, size_t num_categorical_vars, int label_categorical_sigma);
size_t n_cols_1hot_expansion(const cofactor_t **cofactors, size_t n_aggregates, uint32_t **cat_idxs, uint64_t **cat_unique_array, int is_cofactor, int drop_first);
size_t find_in_array(uint64_t a, const uint64_t *array, size_t start, size_t end);

#endif //POSTGRES_DATATYPE_H
