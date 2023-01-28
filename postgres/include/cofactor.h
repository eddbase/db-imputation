#ifndef COFACTOR_H
#define COFACTOR_H

#include <postgres.h>
#include <assert.h>

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

#define SIZE_SCALAR_ARRAY(n) ((n * (n + 3)) >> 1)

inline size_t size_scalar_array(size_t num_cont)
{
    // num_cont * (num_cont + 3) / 2
    return (num_cont * (num_cont + 3)) >> 1;
}

inline size_t size_scalar_array_cont_cont(size_t num_cont)
{
    // num_cont * (num_cont + 1) / 2
    return (num_cont * (num_cont + 1)) >> 1;
}

inline size_t size_relation_array(size_t num_cont, size_t num_cat)
{
    // num_cat + num_cat * num_cont + num_cat * (num_cat - 1) / 2
    return (num_cat * ((num_cont << 1) + num_cat + 1)) >> 1;
}

inline size_t size_relation_array_cat_cat(size_t num_cat)
{
    // num_cat * (num_cat - 1) / 2
    return (num_cat * (num_cat - 1)) >> 1;
}

inline size_t sizeof_cofactor_t(const cofactor_t *c)
{
    return sizeof(cofactor_t) +
           size_scalar_array(c->num_continuous_vars) * sizeof(float8) +
           c->sz_relation_data;
}

inline float8 *scalar_array(cofactor_t *c)
{
    return (float8 *)c->data;
}

inline const float8 *cscalar_array(const cofactor_t *c)
{
    return (const float8 *)c->data;
}

inline char *relation_array(cofactor_t *c)
{
    return (char *)(scalar_array(c) + size_scalar_array(c->num_continuous_vars));
}

inline const char *crelation_array(const cofactor_t *c)
{
    return (const char *)(cscalar_array(c) + size_scalar_array(c->num_continuous_vars));
}

size_t get_num_categories(const cofactor_t *cofactor, int label_categorical_sigma);

size_t sizeof_sigma_matrix(const cofactor_t *cofactor, int label_categorical_sigma);

size_t find_in_array(uint64_t a, const uint64_t *array, size_t start, size_t end);

void build_sigma_matrix(const cofactor_t *cofactor, size_t matrix_size, int label_categorical_sigma,
                        /* out */ float8 *sigma);

#endif // COFACTOR_H
