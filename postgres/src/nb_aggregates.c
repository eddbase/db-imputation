#include "nb_aggregates.h"
#include "serializer.h"

#include <fmgr.h>
#include <catalog/pg_type.h>
#include <utils/array.h>
#include <math.h>

/*****************************************************************************
 * Input/output functions
 *****************************************************************************/


PG_FUNCTION_INFO_V1(read_nb_aggregates);

Datum read_nb_aggregates(PG_FUNCTION_ARGS)
{
    const char *buf = PG_GETARG_CSTRING(0);

    cofactor_t tmp;
    int offset;
    sscanf(buf, "%u, %hu, %hu, %d, %n",
           &tmp.sz_relation_data, &tmp.num_continuous_vars,
           &tmp.num_categorical_vars, &tmp.count, &offset);

    size_t sz_scalar_array = size_scalar_array(tmp.num_continuous_vars);
    size_t sz_scalar_data = sz_scalar_array * sizeof(float8);
    size_t sz_cofactor = sizeof(cofactor_t) + sz_scalar_data + tmp.sz_relation_data;

    // allocate data
    cofactor_t *out = (cofactor_t *)palloc0(sz_cofactor);
    SET_VARSIZE(out, sz_cofactor);

    // set header data
    out->sz_relation_data = tmp.sz_relation_data;
    out->num_continuous_vars = tmp.num_continuous_vars;
    out->num_categorical_vars = tmp.num_categorical_vars;
    out->count = tmp.count;

    size_t sz_relation_array = size_relation_array(tmp.num_categorical_vars);
    read_cofactor_data(buf + offset, sz_scalar_array, sz_relation_array, out->data);

    PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(write_nb_aggregates);

Datum write_nb_aggregates(PG_FUNCTION_ARGS)
{
    cofactor_t *c = (cofactor_t *)PG_GETARG_VARLENA_P(0);

    size_t sz_scalar_array = size_scalar_array(c->num_continuous_vars);
    size_t sz_relation_array = size_relation_array(c->num_categorical_vars);

    // get buffer size
    size_t bufsz = snprintf(NULL, 0, "%u, %hu, %hu, %d, ",
                            c->sz_relation_data, c->num_continuous_vars,
                            c->num_categorical_vars, c->count);
    bufsz += size_for_write_cofactor_data(sz_scalar_array, sz_relation_array, c->data);

    // allocate memory
    char *out = (char *)palloc0((bufsz + 1) * sizeof(char));

    // stringify
    int offset = sprintf(out, "%u, %hu, %hu, %d, ",
                         c->sz_relation_data, c->num_continuous_vars,
                         c->num_categorical_vars, c->count);
    write_cofactor_data(sz_scalar_array, sz_relation_array, c->data, out + offset);

    PG_RETURN_CSTRING(out);
}

/*****************************************************************************
 * Algebraic functions
 *****************************************************************************/

static cofactor_t *union_cofactors(const cofactor_t *a,
                            const cofactor_t *b,
                            union_relations_fn_t union_relations_fn)
{
#ifdef DEBUG_COFACTOR
    assert(a->num_continuous_vars == b->num_continuous_vars &&
           a->num_categorical_vars == b->num_categorical_vars);
#endif

    size_t sz_scalar_array = size_scalar_array(a->num_continuous_vars);
    size_t sz_relation_array = size_relation_array(a->num_categorical_vars);

    size_t sz_scalar_data = sz_scalar_array * sizeof(float8);
    size_t max_sz_relation_data = a->sz_relation_data + b->sz_relation_data - sz_relation_array * sizeof(relation_t);
    size_t sz = sizeof(cofactor_t) + sz_scalar_data + max_sz_relation_data;

    // allocate
    cofactor_t *out = (cofactor_t *)palloc0(sz);
    SET_VARSIZE(out, sz);

    // set header data
    out->sz_relation_data = 0;
    out->num_continuous_vars = a->num_continuous_vars;
    out->num_categorical_vars = a->num_categorical_vars;
    out->count = a->count + b->count;

    // add scalar arrays
    for (size_t i = 0; i < sz_scalar_array; i++)
    {
        scalar_array(out)[i] = cscalar_array(a)[i] + cscalar_array(b)[i];
    }

    // add relation arrays
    const char *a_relation_array = (const char *)(cscalar_array(a) + sz_scalar_array);
    const char *b_relation_array = (const char *)(cscalar_array(b) + sz_scalar_array);
    char *out_relation_array = (char *)(scalar_array(out) + sz_scalar_array);
    for (size_t i = 0; i < sz_relation_array; i++)
    {
        const relation_t *a_relation = (const relation_t *)a_relation_array;
        const relation_t *b_relation = (const relation_t *)b_relation_array;
        relation_t *out_relation = (relation_t *)out_relation_array;

        union_relations_fn(a_relation, b_relation, out_relation);

        a_relation_array += a_relation->sz_struct;
        b_relation_array += b_relation->sz_struct;
        out_relation_array += out_relation->sz_struct;
        out->sz_relation_data += out_relation->sz_struct;
    }

#ifdef DEBUG_COFACTOR
    size_t actual_sz = sizeof_cofactor_t(out);
    assert(actual_sz <= sz);
    if (actual_sz < sz)
    {
        // no action -- union can compact tuples
        // elog(WARNING, "actual < max");
    }
#endif

    return out;
}

static cofactor_t *difference_cofactors(const cofactor_t *a,
                                 const cofactor_t *b,
                                 union_relations_fn_t union_relations_fn)
{
#ifdef DEBUG_COFACTOR
    assert(a->num_continuous_vars == b->num_continuous_vars &&
           a->num_categorical_vars == b->num_categorical_vars);
#endif

    size_t sz_scalar_array = size_scalar_array(a->num_continuous_vars);
    size_t sz_relation_array = size_relation_array(a->num_categorical_vars);

    size_t sz_scalar_data = sz_scalar_array * sizeof(float8);
    size_t max_sz_relation_data = a->sz_relation_data + b->sz_relation_data - sz_relation_array * sizeof(relation_t);
    size_t sz = sizeof(cofactor_t) + sz_scalar_data + max_sz_relation_data;

    // allocate
    cofactor_t *out = (cofactor_t *)palloc0(sz);
    SET_VARSIZE(out, sz);

    // set header data
    out->sz_relation_data = 0;
    out->num_continuous_vars = a->num_continuous_vars;
    out->num_categorical_vars = a->num_categorical_vars;
    out->count = a->count - b->count;

    // add scalar arrays
    for (size_t i = 0; i < sz_scalar_array; i++)
    {
        scalar_array(out)[i] = cscalar_array(a)[i] - cscalar_array(b)[i];
    }

    // add relation arrays
    const char *a_relation_array = (const char *)(cscalar_array(a) + sz_scalar_array);
    const char *b_relation_array = (const char *)(cscalar_array(b) + sz_scalar_array);
    char *out_relation_array = (char *)(scalar_array(out) + sz_scalar_array);
    for (size_t i = 0; i < sz_relation_array; i++)
    {
        const relation_t *a_relation = (const relation_t *)a_relation_array;
        const relation_t *b_relation = (const relation_t *)b_relation_array;
        relation_t *out_relation = (relation_t *)out_relation_array;

        union_relations_fn(a_relation, b_relation, out_relation);

        a_relation_array += a_relation->sz_struct;
        b_relation_array += b_relation->sz_struct;
        out_relation_array += out_relation->sz_struct;
        out->sz_relation_data += out_relation->sz_struct;
    }

#ifdef DEBUG_COFACTOR
    size_t actual_sz = sizeof_cofactor_t(out);
    assert(actual_sz <= sz);
    if (actual_sz < sz)
    {
        // no action -- union can compact tuples
        // elog(WARNING, "actual < max");
    }
#endif

    return out;
}


PG_FUNCTION_INFO_V1(pg_add_nb_aggregates);

Datum pg_add_nb_aggregates(PG_FUNCTION_ARGS)
{
    const cofactor_t *const a = (cofactor_t *)PG_GETARG_VARLENA_P(0);
    const cofactor_t *const b = (cofactor_t *)PG_GETARG_VARLENA_P(1);
    cofactor_t *out = union_cofactors(a, b, &add_relations);
    PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(pg_sub_nb_aggregates);

Datum pg_sub_nb_aggregates(PG_FUNCTION_ARGS)
{
    const cofactor_t *const a = (cofactor_t *)PG_GETARG_VARLENA_P(0);
    const cofactor_t *const b = (cofactor_t *)PG_GETARG_VARLENA_P(1);
    cofactor_t *out = difference_cofactors(a, b, &subtract_relations);
    PG_RETURN_POINTER(out);
}

static size_t size_for_multiply_cofactors(const cofactor_t *a, const cofactor_t *b)
{
    size_t out_sz_scalar_array = size_scalar_array(a->num_continuous_vars + b->num_continuous_vars);
    size_t sz_total = sizeof(cofactor_t) +
                      out_sz_scalar_array * sizeof(float8) +     // scalar array
                      a->sz_relation_data + b->sz_relation_data; // scaled relation parts

    return sz_total;
}

static void multiply_scalar_arrays(const cofactor_t *a, const cofactor_t *b,
        /* out */ float8 *out)
{
    // - degree 1
    const float8 *const a_sum1_array = cscalar_array(a);
    for (size_t i = 0; i < a->num_continuous_vars; i++)
    {
        *out++ = b->count * a_sum1_array[i];
    }
    const float8 *const b_sum1_array = cscalar_array(b);
    for (size_t i = 0; i < b->num_continuous_vars; i++)
    {
        *out++ = a->count * b_sum1_array[i];
    }
}

static size_t multiply_relation_arrays(const cofactor_t *a, const cofactor_t *b,
        /* out */ char *out)
{
    const char *const a_relation_array = crelation_array(a);
    const char *const b_relation_array = crelation_array(b);
    const char *const out_start = out;

    // degree 1
    const char *a_sum1_relation_array = a_relation_array;
    for (size_t i = 0; i < a->num_categorical_vars; i++)
    {
        const relation_t *a_relation = (const relation_t *)a_sum1_relation_array;
        relation_t *out_relation = (relation_t *)out;

        scale_relation(a_relation, b->count, out_relation);

        out += out_relation->sz_struct;
        a_sum1_relation_array += a_relation->sz_struct;
    }

    const char *b_sum1_relation_array = b_relation_array;
    for (size_t i = 0; i < b->num_categorical_vars; i++)
    {
        const relation_t *b_relation = (const relation_t *)b_sum1_relation_array;
        relation_t *out_relation = (relation_t *)out;

        scale_relation(b_relation, a->count, out_relation);

        out += out_relation->sz_struct;
        b_sum1_relation_array += b_relation->sz_struct;
    }

    return out - out_start;
}

PG_FUNCTION_INFO_V1(pg_multiply_nb_aggregates);

Datum pg_multiply_nb_aggregates(PG_FUNCTION_ARGS)
{
    cofactor_t *a = (cofactor_t *)PG_GETARG_VARLENA_P(0);
    cofactor_t *b = (cofactor_t *)PG_GETARG_VARLENA_P(1);

    // allocate data
    size_t sz = size_for_multiply_cofactors(a, b);
    cofactor_t *out = (cofactor_t *)palloc0(sz);
    SET_VARSIZE(out, sz);

    out->num_continuous_vars = a->num_continuous_vars + b->num_continuous_vars;
    out->num_categorical_vars = a->num_categorical_vars + b->num_categorical_vars;
    out->count = a->count * b->count;

    // multiply scalar arrays
    multiply_scalar_arrays(a, b, scalar_array(out));

    // multiply relation arrays
    size_t rel_sz = multiply_relation_arrays(a, b, relation_array(out));
    out->sz_relation_data = rel_sz;

#ifdef DEBUG_COFACTOR
    size_t actual_sz = sizeof_cofactor_t(out);
    assert(actual_sz == sz);
#endif

    PG_RETURN_POINTER(out);
}

/*****************************************************************************
 * Lift functions
 *****************************************************************************/


PG_FUNCTION_INFO_V1(lift_to_nb_aggregates);

Datum lift_to_nb_aggregates(PG_FUNCTION_ARGS)
{
    ArrayType *cont_array = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *cat_array = PG_GETARG_ARRAYTYPE_P(1);

    int num_cont = (ARR_NDIM(cont_array) == 1 ? ARR_DIMS(cont_array)[0] : 0);
    int num_cat = (ARR_NDIM(cat_array) == 1 ? ARR_DIMS(cat_array)[0] : 0);

    size_t sz_scalar_array = size_scalar_array(num_cont);
    size_t sz_scalar_data = sz_scalar_array * sizeof(float8);
    size_t sz_relation_array = size_relation_array(num_cat);
    size_t sz_relation_data = sz_relation_array * SIZEOF_RELATION(1);
    size_t sz_cofactor = sizeof(cofactor_t) + sz_scalar_data + sz_relation_data;
    cofactor_t *out = (cofactor_t *)palloc0(sz_cofactor);
    SET_VARSIZE(out, sz_cofactor);

    out->sz_relation_data = sz_relation_data;
    out->num_continuous_vars = num_cont;
    out->num_categorical_vars = num_cat;
    out->count = 1;

    const float8 *cont_values = (float8 *)ARR_DATA_PTR(cont_array);
    float8 *out_scalar_array = scalar_array(out);

    switch (num_cont)
    {
        case 0:
            break;
        case 1: {
            float8 a = cont_values[0];
            out_scalar_array[0] = a;
            out_scalar_array[1] = a * a;
            break;
        }
        default:
        {
            for (size_t i = 0; i < num_cont; i++){
                *out_scalar_array++ = cont_values[i];
            }
            for (size_t i = 0; i < num_cont; i++){
                *out_scalar_array++ = cont_values[i] * cont_values[i];
            }
        }
    };

    uint32_t *cat_values = (uint32_t *)ARR_DATA_PTR(cat_array);
    char *out_relation_array = relation_array(out);
    switch (num_cat)
    {
        case 0:
            break;
        case 1:
        {

            relation_t *r = (relation_t *)out_relation_array;
            r->sz_struct = SIZEOF_RELATION(1);
            r->num_tuples = 1;
            r->tuples[0].key = cat_values[0];
            r->tuples[0].value = 1.0;
            out_relation_array += r->sz_struct;
            break;
        }
        default:
        {
            // cat
            for (size_t i = 0; i < num_cat; i++)
            {
                relation_t *r = (relation_t *)out_relation_array;
                r->sz_struct = SIZEOF_RELATION(1);
                r->num_tuples = 1;
                r->tuples[0].key = cat_values[i];
                r->tuples[0].value = 1.0;
                out_relation_array += r->sz_struct;
            }
        }
    }

    PG_RETURN_POINTER(out);
}

