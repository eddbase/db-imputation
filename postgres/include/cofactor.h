#ifndef COFACTOR_H
#define COFACTOR_H

#include <postgres.h>
#include <assert.h>
#include <datatype.h>

#define SIZE_SCALAR_ARRAY(n) ((n * (n + 3)) >> 1)



inline size_t size_scalar_array_cont_cont(size_t num_cont)
{
    // num_cont * (num_cont + 1) / 2
    return (num_cont * (num_cont + 1)) >> 1;
}

inline size_t size_relation_array_cat_cat(size_t num_cat)
{
    // num_cat * (num_cat - 1) / 2
    return (num_cat * (num_cat - 1)) >> 1;
}

inline size_t size_relation_array(size_t num_cont, size_t num_cat)
{
    return (num_cat * ((num_cont << 1) + num_cat + 1)) >> 1;
}

inline size_t size_scalar_array(size_t num_cont)
{
    return (num_cont * (num_cont + 3)) >> 1;
}

inline size_t sizeof_cofactor_t(const cofactor_t *c)
{
    return sizeof(cofactor_t) +
           size_scalar_array(c->num_continuous_vars) * sizeof(float8) +
           c->sz_relation_data;
}

inline char *relation_array(cofactor_t *c)
{
    return (char *)(scalar_array(c) + size_scalar_array(c->num_continuous_vars));
}

inline const char *crelation_array(const cofactor_t *c)
{
    return (const char *)(cscalar_array(c) + size_scalar_array(c->num_continuous_vars));
}

size_t sizeof_sigma_matrix(const cofactor_t *cofactor, int label_categorical_sigma);

size_t find_in_array(uint64_t a, const uint64_t *array, size_t start, size_t end);

void build_sigma_matrix(const cofactor_t *cofactor, size_t matrix_size, int label_categorical_sigma, uint64_t *cat_array, uint32_t *cat_vars_idxs, int drop_first,
                        /* out */ float8 *sigma);
void build_sum_matrix(const cofactor_t *cofactor, size_t num_total_params, int label, uint64_t *cat_array, uint32_t *cat_vars_idxs, int drop_first,
        /* out */ double *sum_vector);
void build_sum_vector(const cofactor_t *cofactor, size_t num_total_params, uint64_t *cat_array, uint32_t *cat_vars_idxs, int drop_first,
        /* out */ double *sum_vector);

#endif // COFACTOR_H
