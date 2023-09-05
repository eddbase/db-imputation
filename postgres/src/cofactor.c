#include "cofactor.h"
#include "serializer.h"

#include <fmgr.h>
#include <catalog/pg_type.h>
#include <utils/array.h>
#include <math.h>

/*****************************************************************************
 * Input/output functions
 *****************************************************************************/

PG_FUNCTION_INFO_V1(read_cofactor);

Datum read_cofactor(PG_FUNCTION_ARGS)
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

    size_t sz_relation_array = size_relation_array(tmp.num_continuous_vars, tmp.num_categorical_vars);
    read_cofactor_data(buf + offset, sz_scalar_array, sz_relation_array, out->data);

    PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(write_cofactor);

Datum write_cofactor(PG_FUNCTION_ARGS)
{
    cofactor_t *c = (cofactor_t *)PG_GETARG_VARLENA_P(0);

    size_t sz_scalar_array = size_scalar_array(c->num_continuous_vars);
    size_t sz_relation_array = size_relation_array(c->num_continuous_vars, c->num_categorical_vars);

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

cofactor_t *union_cofactors(const cofactor_t *a,
                            const cofactor_t *b,
                            union_relations_fn_t union_relations_fn)
{
#ifdef DEBUG_COFACTOR
    assert(a->num_continuous_vars == b->num_continuous_vars &&
           a->num_categorical_vars == b->num_categorical_vars);
#endif

    size_t sz_scalar_array = size_scalar_array(a->num_continuous_vars);
    size_t sz_relation_array = size_relation_array(a->num_continuous_vars, a->num_categorical_vars);

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

cofactor_t *difference_cofactors(const cofactor_t *a,
                                 const cofactor_t *b,
                                 union_relations_fn_t union_relations_fn)
{
#ifdef DEBUG_COFACTOR
    assert(a->num_continuous_vars == b->num_continuous_vars &&
           a->num_categorical_vars == b->num_categorical_vars);
#endif

    size_t sz_scalar_array = size_scalar_array(a->num_continuous_vars);
    size_t sz_relation_array = size_relation_array(a->num_continuous_vars, a->num_categorical_vars);

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


PG_FUNCTION_INFO_V1(pg_add_cofactors);

Datum pg_add_cofactors(PG_FUNCTION_ARGS)
{
    const cofactor_t *const a = (cofactor_t *)PG_GETARG_VARLENA_P(0);
    const cofactor_t *const b = (cofactor_t *)PG_GETARG_VARLENA_P(1);
    cofactor_t *out = union_cofactors(a, b, &add_relations);
    PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(pg_subtract_cofactors);

Datum pg_subtract_cofactors(PG_FUNCTION_ARGS)
{
    const cofactor_t *const a = (cofactor_t *)PG_GETARG_VARLENA_P(0);
    const cofactor_t *const b = (cofactor_t *)PG_GETARG_VARLENA_P(1);
    cofactor_t *out = difference_cofactors(a, b, &subtract_relations);
    PG_RETURN_POINTER(out);
}

size_t size_for_multiply_cofactors(const cofactor_t *a, const cofactor_t *b)
{
    size_t out_sz_scalar_array = size_scalar_array(a->num_continuous_vars + b->num_continuous_vars);
    size_t sz_total = sizeof(cofactor_t) +
                      out_sz_scalar_array * sizeof(float8) +     // scalar array
                      a->sz_relation_data + b->sz_relation_data; // scaled relation parts

    const char *const a_relation_array = crelation_array(a);
    const char *const b_relation_array = crelation_array(b);

    // cat_a * cont_b
    if (b->num_continuous_vars > 0)
    {
        const char *a_relation_array_end = a_relation_array;
        for (size_t i = 0; i < a->num_categorical_vars; i++)
        {
            const relation_t *a_relation = (const relation_t *)a_relation_array_end;
            a_relation_array_end += a_relation->sz_struct;
        }
        sz_total += (size_t)(a_relation_array_end - a_relation_array) * b->num_continuous_vars;
    }

    // cont_a * cat_b
    if (a->num_continuous_vars > 0)
    {
        const char *b_relation_array_end = b_relation_array;
        for (size_t i = 0; i < b->num_categorical_vars; i++)
        {
            const relation_t *b_relation = (const relation_t *)b_relation_array_end;
            b_relation_array_end += b_relation->sz_struct;
        }
        sz_total += a->num_continuous_vars * (size_t)(b_relation_array_end - b_relation_array);
    }

    // cat_a * cat_b
    const char *a_relation_array_end = a_relation_array;
    for (size_t i = 0; i < a->num_categorical_vars; i++)
    {
        const relation_t *a_relation = (const relation_t *)a_relation_array_end;
        const char *b_relation_array_end = b_relation_array;
        for (size_t j = 0; j < b->num_categorical_vars; j++)
        {
            const relation_t *b_relation = (const relation_t *)b_relation_array_end;
            sz_total += sizeof_relation_t(a_relation->num_tuples * b_relation->num_tuples);
            b_relation_array_end += b_relation->sz_struct;
        }
        a_relation_array_end += a_relation->sz_struct;
    }

    return sz_total;
}

void multiply_scalar_arrays(const cofactor_t *a, const cofactor_t *b,
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

    // - degree 2
    const float8 *a_sum2_array = a_sum1_array + a->num_continuous_vars;
    for (size_t i = 0; i < a->num_continuous_vars; i++)
    {
        for (size_t j = i; j < a->num_continuous_vars; j++)
        {
            *out++ = (*a_sum2_array++) * b->count;
        }
        for (size_t j = 0; j < b->num_continuous_vars; j++)
        {
            *out++ = a_sum1_array[i] * b_sum1_array[j];
        }
    }
    const float8 *b_sum2_array = b_sum1_array + b->num_continuous_vars;
    for (size_t i = 0; i < size_scalar_array_cont_cont(b->num_continuous_vars); i++)
    {
        *out++ = (*b_sum2_array++) * a->count;
    }
}

size_t multiply_relation_arrays(const cofactor_t *a, const cofactor_t *b,
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

    // degree 2
    const char *a_sum2_relation_array = (const char *)a_sum1_relation_array;
    const char *b_sum2_relation_array = (const char *)b_sum1_relation_array;

    const float8 *const a_sum1_scalar_array = cscalar_array(a);
    for (size_t i = 0; i < a->num_continuous_vars; i++)
    {
        // (cont_A * cat_A) * count_B
        for (size_t j = 0; j < a->num_categorical_vars; j++)
        {
            const relation_t *a_relation = (const relation_t *)a_sum2_relation_array;
            relation_t *out_relation = (relation_t *)out;

            scale_relation(a_relation, b->count, out_relation);

            out += out_relation->sz_struct;
            a_sum2_relation_array += a_relation->sz_struct;
        }

        // cont_A * cat_B
        b_sum1_relation_array = b_relation_array;
        for (size_t j = 0; j < b->num_categorical_vars; j++)
        {
            const relation_t *b_relation = (const relation_t *)b_sum1_relation_array;
            relation_t *out_relation = (relation_t *)out;

            scale_relation(b_relation, a_sum1_scalar_array[i], out_relation);

            out += out_relation->sz_struct;
            b_sum1_relation_array += b_relation->sz_struct;
        }
    }

    const float8 *const b_sum1_scalar_array = cscalar_array(b);
    for (size_t i = 0; i < b->num_continuous_vars; i++)
    {
        // cont_B * cat_A
        a_sum1_relation_array = a_relation_array;
        for (size_t j = 0; j < a->num_categorical_vars; j++)
        {
            const relation_t *a_relation = (const relation_t *)a_sum1_relation_array;
            relation_t *out_relation = (relation_t *)out;

            scale_relation(a_relation, b_sum1_scalar_array[i], out_relation);

            out += out_relation->sz_struct;
            a_sum1_relation_array += a_relation->sz_struct;
        }

        // (cont_B * cat_B) * count_A
        for (size_t j = 0; j < b->num_categorical_vars; j++)
        {
            const relation_t *b_relation = (const relation_t *)b_sum2_relation_array;
            relation_t *out_relation = (relation_t *)out;

            scale_relation(b_relation, a->count, out_relation);

            out += out_relation->sz_struct;
            b_sum2_relation_array += b_relation->sz_struct;
        }
    }

    a_sum1_relation_array = a_relation_array;
    for (size_t i = 0; i < a->num_categorical_vars; i++)
    {
        // (cat_A * cat_A) * count_B
        for (size_t j = i + 1; j < a->num_categorical_vars; j++)
        {
            const relation_t *a_relation = (const relation_t *)a_sum2_relation_array;
            relation_t *out_relation = (relation_t *)out;

            scale_relation(a_relation, b->count, out_relation);

            out += out_relation->sz_struct;
            a_sum2_relation_array += a_relation->sz_struct;
        }

        // cat_A * cat_B
        const relation_t *a_relation = (const relation_t *)a_sum1_relation_array;
        b_sum1_relation_array = b_relation_array;
        for (size_t j = 0; j < b->num_categorical_vars; j++)
        {
            const relation_t *b_relation = (const relation_t *)b_sum1_relation_array;
            relation_t *out_relation = (relation_t *)out;

            multiply_relations(a_relation, b_relation, out_relation);

            out += out_relation->sz_struct;
            b_sum1_relation_array += b_relation->sz_struct;
        }
        a_sum1_relation_array += a_relation->sz_struct;
    }

    // (cat_B * cat_B) * count_A
    for (size_t i = 0; i < size_relation_array_cat_cat(b->num_categorical_vars); i++)
    {
        const relation_t *b_relation = (const relation_t *)b_sum2_relation_array;
        relation_t *out_relation = (relation_t *)out;

        scale_relation(b_relation, a->count, out_relation);

        out += out_relation->sz_struct;
        b_sum2_relation_array += b_relation->sz_struct;
    }

    return out - out_start;
}

PG_FUNCTION_INFO_V1(pg_multiply_cofactors);

Datum pg_multiply_cofactors(PG_FUNCTION_ARGS)
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

PG_FUNCTION_INFO_V1(lift_const_to_cofactor);

Datum lift_const_to_cofactor(PG_FUNCTION_ARGS)
{
    size_t sz_cofactor = sizeof(cofactor_t);

    cofactor_t *out = (cofactor_t *)palloc0(sz_cofactor);
    SET_VARSIZE(out, sz_cofactor);

    out->sz_relation_data = 0;
    out->num_continuous_vars = 0;
    out->num_categorical_vars = 0;
    out->count = PG_GETARG_INT32(0);

    PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(lift_cont_to_cofactor);

Datum lift_cont_to_cofactor(PG_FUNCTION_ARGS)
{
    size_t sz_scalar_data = SIZE_SCALAR_ARRAY(1) * sizeof(float8);
    size_t sz_relation_data = 0;
    size_t sz_cofactor = sizeof(cofactor_t) + sz_scalar_data + sz_relation_data;

    cofactor_t *out = (cofactor_t *)palloc0(sz_cofactor);
    SET_VARSIZE(out, sz_cofactor);

    out->sz_relation_data = 0;
    out->num_continuous_vars = 1;
    out->num_categorical_vars = 0;
    out->count = 1;

    float8 a = PG_GETARG_FLOAT8(0);
    float8 *out_array = (float8 *)out->data;
    out_array[0] = a;
    out_array[1] = a * a;

    PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(lift_cat_to_cofactor);

Datum lift_cat_to_cofactor(PG_FUNCTION_ARGS)
{
    size_t sz_scalar_data = 0;
    size_t sz_relation_data = SIZEOF_RELATION(1);
    size_t sz_cofactor = sizeof(cofactor_t) + sz_scalar_data + sz_relation_data;

    cofactor_t *out = (cofactor_t *)palloc0(sz_cofactor);
    SET_VARSIZE(out, sz_cofactor);

    out->sz_relation_data = sz_relation_data;
    out->num_continuous_vars = 0;
    out->num_categorical_vars = 1;
    out->count = 1;

    relation_t *r = (relation_t *)out->data;
    r->sz_struct = SIZEOF_RELATION(1);
    r->num_tuples = 1;
    r->tuples[0].key = PG_GETARG_UINT32(0);
    r->tuples[0].value = 1.0;

    PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(lift_to_cofactor);

Datum lift_to_cofactor(PG_FUNCTION_ARGS)
{
    ArrayType *cont_array = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *cat_array = PG_GETARG_ARRAYTYPE_P(1);

    int num_cont = (ARR_NDIM(cont_array) == 1 ? ARR_DIMS(cont_array)[0] : 0);
    int num_cat = (ARR_NDIM(cat_array) == 1 ? ARR_DIMS(cat_array)[0] : 0);

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
    out->count = 1;

    const float8 *cont_values = (float8 *)ARR_DATA_PTR(cont_array);
    float8 *out_scalar_array = scalar_array(out);

    switch (num_cont)
    {
        case 0:
            break;
        case 1:
        {
            float8 a = cont_values[0];
            out_scalar_array[0] = a;
            out_scalar_array[1] = a * a;
            break;
        }
            // case 5:
            // {
            //     float8 a = cont_values[0];
            //     float8 b = cont_values[1];
            //     float8 c = cont_values[2];
            //     float8 d = cont_values[3];
            //     float8 e = cont_values[4];

            //     out_scalar_array[0] = a;
            //     out_scalar_array[1] = b;
            //     out_scalar_array[2] = c;
            //     out_scalar_array[3] = d;
            //     out_scalar_array[4] = e;
            //     out_scalar_array[5] = a * a;
            //     out_scalar_array[6] = a * b;
            //     out_scalar_array[7] = a * c;
            //     out_scalar_array[8] = a * d;
            //     out_scalar_array[9] = a * e;
            //     out_scalar_array[10] = b * b;
            //     out_scalar_array[11] = b * c;
            //     out_scalar_array[12] = b * d;
            //     out_scalar_array[13] = b * e;
            //     out_scalar_array[14] = c * c;
            //     out_scalar_array[15] = c * d;
            //     out_scalar_array[16] = c * e;
            //     out_scalar_array[17] = d * d;
            //     out_scalar_array[18] = d * e;
            //     out_scalar_array[19] = e * e;
            //     break;
            // }
            // case 6:
            // {
            //     float8 a = cont_values[0];
            //     float8 b = cont_values[1];
            //     float8 c = cont_values[2];
            //     float8 d = cont_values[3];
            //     float8 e = cont_values[4];
            //     float8 f = cont_values[5];

            //     out_scalar_array[0] = a;
            //     out_scalar_array[1] = b;
            //     out_scalar_array[2] = c;
            //     out_scalar_array[3] = d;
            //     out_scalar_array[4] = e;
            //     out_scalar_array[5] = f;
            //     out_scalar_array[6] = a * a;
            //     out_scalar_array[7] = a * b;
            //     out_scalar_array[8] = a * c;
            //     out_scalar_array[9] = a * d;
            //     out_scalar_array[10] = a * e;
            //     out_scalar_array[11] = a * f;
            //     out_scalar_array[12] = b * b;
            //     out_scalar_array[13] = b * c;
            //     out_scalar_array[14] = b * d;
            //     out_scalar_array[15] = b * e;
            //     out_scalar_array[16] = b * f;
            //     out_scalar_array[17] = c * c;
            //     out_scalar_array[18] = c * d;
            //     out_scalar_array[19] = c * e;
            //     out_scalar_array[20] = c * f;
            //     out_scalar_array[21] = d * d;
            //     out_scalar_array[22] = d * e;
            //     out_scalar_array[23] = d * f;
            //     out_scalar_array[24] = e * e;
            //     out_scalar_array[25] = e * f;
            //     out_scalar_array[26] = f * f;
            //     break;
            // }
        default:
        {
            for (size_t i = 0; i < num_cont; i++)
            {
                *out_scalar_array++ = cont_values[i];
            }
            for (size_t i = 0; i < num_cont; i++)
            {
                for (size_t j = i; j < num_cont; j++)
                {
                    *out_scalar_array++ = cont_values[i] * cont_values[j];
                }
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

            for (size_t i = 0; i < num_cont; i++)
            {
                r = (relation_t *)out_relation_array;
                r->sz_struct = SIZEOF_RELATION(1);
                r->num_tuples = 1;
                r->tuples[0].key = cat_values[0];
                r->tuples[0].value = cont_values[i];
                out_relation_array += r->sz_struct;
            }
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
            // cont * cat
            for (size_t i = 0; i < num_cont; i++)
            {
                for (size_t j = 0; j < num_cat; j++)
                {
                    relation_t *r = (relation_t *)out_relation_array;
                    r->sz_struct = SIZEOF_RELATION(1);
                    r->num_tuples = 1;
                    r->tuples[0].key = cat_values[j];
                    r->tuples[0].value = cont_values[i];
                    out_relation_array += r->sz_struct;
                }
            }
            // cat * cat
            for (size_t i = 0; i < num_cat; i++)
            {
                for (size_t j = i + 1; j < num_cat; j++)
                {
                    relation_t *r = (relation_t *)out_relation_array;
                    r->sz_struct = SIZEOF_RELATION(1);
                    r->num_tuples = 1;
                    r->tuples[0].slots[0] = cat_values[i];
                    r->tuples[0].slots[1] = cat_values[j];
                    r->tuples[0].value = 1.0;
                    out_relation_array += r->sz_struct;
                }
            }
        }
    }

    PG_RETURN_POINTER(out);
}

/*****************************************************************************
 * Statistics functions
 *****************************************************************************/

PG_FUNCTION_INFO_V1(pg_cofactor_stats);

Datum pg_cofactor_stats(PG_FUNCTION_ARGS)
{
    cofactor_t *a = (cofactor_t *)PG_GETARG_VARLENA_P(0);

    const char * a_relation_array = crelation_array(a);
    size_t sz_relation_array =
            size_relation_array(a->num_continuous_vars, a->num_categorical_vars);

    size_t total_tuples = 0, total_squared_tuples = 0, max_tuples = 0;
    size_t min_tuples = ((const relation_t *)a_relation_array)->num_tuples;
    for (size_t i = 0; i < sz_relation_array; i++) {
        const relation_t *a_relation = (const relation_t *)a_relation_array;
        total_tuples += a_relation->num_tuples;
        total_squared_tuples += a_relation->num_tuples * a_relation->num_tuples;
        max_tuples = (max_tuples >= a_relation->num_tuples ? max_tuples : a_relation->num_tuples);
        min_tuples = (min_tuples <= a_relation->num_tuples ? min_tuples : a_relation->num_tuples);
        a_relation_array += a_relation->sz_struct;
    }
    float8 avg_tuples = (float8) total_tuples / sz_relation_array;
    float8 stdev_tuples = sqrt((float8) total_squared_tuples / sz_relation_array - avg_tuples * avg_tuples);
/*
    elog(INFO, "num_cont_vars = %hu, num_cat_vars = %hu", a->num_continuous_vars, a->num_categorical_vars);
    elog(INFO, "num_categories = %zu", get_num_categories(a, -1));
    elog(INFO, "num_relations = %zu", sz_relation_array);
    elog(INFO, "total_tuples = %zu, \
                avg = %f, \
                stdev = %f, \
                max = %zu, \
                min = %zu", \
                total_tuples, avg_tuples, stdev_tuples, max_tuples, min_tuples);*/

    PG_RETURN_VOID();
}


/*****************************************************************************
 * Sigma matrix functions
 *****************************************************************************/

size_t sizeof_sigma_matrix(const cofactor_t *cofactor, int label_categorical_sigma)
{
    // count :: numerical :: 1-hot_categories
    return 1 + cofactor->num_continuous_vars + get_num_categories(crelation_array(cofactor), cofactor->num_categorical_vars, label_categorical_sigma);
}

//sum without group by
void build_sum_vector(const cofactor_t *cofactor, size_t num_total_params, uint64_t *cat_array, uint32_t *cat_vars_idxs, int drop_first,
        /* out */ double *sum_vector)
{
    //allocates values in sum vector, each sum array is sorted in tuple order
    relation_t *current_label;
    size_t search_start = 0;        // within one category class
    size_t search_end = search_start;
    uint64_t *classes_order;

    //add count
    sum_vector[0] = cofactor->count;

    const float8 *sum1_scalar_array = cscalar_array(cofactor);

    //numerical features
    for (size_t numerical = 0; numerical < cofactor->num_continuous_vars; numerical++) {
        sum_vector[numerical+1] = sum1_scalar_array[numerical];
    }

    //categorical feats.
    const char *relation_data = crelation_array(cofactor);
    for (size_t i = 0; i < cofactor->num_categorical_vars; i++)
    {
        relation_t *r = (relation_t *) relation_data;
        for (size_t j = 0; j < r->num_tuples; j++)
        {
            // search key index
            size_t key_index = find_in_array(r->tuples[j].key, cat_array, cat_vars_idxs[i], cat_vars_idxs[i + 1]);
            if (drop_first && key_index == cat_vars_idxs[i + 1])
                continue;//skip

            assert(key_index < cat_vars_idxs[i + 1]);
            sum_vector[key_index + cofactor->num_continuous_vars + 1] = r->tuples[j].value;
        }
        relation_data += r->sz_struct;
    }
}


//generate a vector of sum of attributes group by label (SUM GROUP BY LABEL)
//returns count, sum of numerical attributes, sum of categorical attributes 1-hot encoded
void build_sum_matrix(const cofactor_t *cofactor, size_t num_total_params, int label, uint64_t *cat_array, uint32_t *cat_vars_idxs, int drop_first,
        /* out */ double *sum_vector)
{
    //allocates values in sum vector, each sum array is sorted in tuple order
    const char *relation_data = crelation_array(cofactor);
    relation_t *current_label;
    size_t search_start = 0;        // within one category class
    size_t search_end = search_start;
    uint64_t *classes_order;

    relation_data = crelation_array(cofactor);

    //add count
    for (size_t i = 0; i < cofactor->num_categorical_vars; i++){
        relation_t *r = (relation_t *) relation_data;
        if (i == label) {//1*label
            current_label = r;
            //sort keys
            for (size_t j=0;j<current_label->num_tuples;j++) {
                size_t idx_key = find_in_array(current_label->tuples[j].key, cat_array, cat_vars_idxs[label], cat_vars_idxs[label+1])-cat_vars_idxs[label];//get index of other class, from 0 to classes
                if (drop_first && idx_key == current_label->num_tuples)
                    continue;//skip
                assert(idx_key < current_label->num_tuples);
                sum_vector[(idx_key*num_total_params)] = r->tuples[j].value;//count in col. 0
            }
        }

        //search_start = search_end;
        relation_data += r->sz_struct;
    }
    //numerical features
    for (size_t numerical = 1; numerical < cofactor->num_continuous_vars + 1; numerical++) {
        for (size_t categorical = 0; categorical < cofactor->num_categorical_vars; categorical++) {
            //sum numerical group by label, copy value in sums vector
            relation_t *r = (relation_t *) relation_data;
            if (categorical != label) {
                relation_data += r->sz_struct;
                continue;
            }
            for (size_t j = 0; j < r->num_tuples; j++) {//a numerical * label relation
                size_t group_by_index = find_in_array(r->tuples[j].key, cat_array, cat_vars_idxs[label], cat_vars_idxs[label+1]) - cat_vars_idxs[label];
                if (drop_first && group_by_index == r->num_tuples)
                    continue;//skip
                sum_vector[(group_by_index * num_total_params) + numerical] = r->tuples[j].value;
            }
            relation_data += r->sz_struct;
        }
    }
    //group by categorical*categorical
    for (size_t curr_cat_var = 0; curr_cat_var < cofactor->num_categorical_vars; curr_cat_var++){
        for (size_t other_cat_var = curr_cat_var + 1; other_cat_var < cofactor->num_categorical_vars; other_cat_var++){
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

            //one of the two is the label
            for (size_t j = 0; j < r->num_tuples; j++)
            {
                size_t key_index_other_var = find_in_array((uint64_t) r->tuples[j].slots[idx_other], cat_array, cat_vars_idxs[other_cat], cat_vars_idxs[other_cat + 1]);
                if (drop_first && key_index_other_var == cat_vars_idxs[other_cat + 1])
                    continue;//skip

                assert(key_index_other_var < cat_vars_idxs[other_cat + 1]);
                key_index_other_var += cofactor->num_continuous_vars + 1;

                size_t group_by_index = find_in_array((uint64_t) r->tuples[j].slots[idx_current], cat_array, cat_vars_idxs[label], cat_vars_idxs[label + 1]) - cat_vars_idxs[label];
                if (drop_first && group_by_index == cat_vars_idxs[label + 1] - cat_vars_idxs[label])
                    continue;//skip

                assert(group_by_index < cat_vars_idxs[label + 1] - cat_vars_idxs[label]);
                sum_vector[(group_by_index*num_total_params)+key_index_other_var] = r->tuples[j].value;
            }
            relation_data += r->sz_struct;
        }
    }
}

//if label_categorical_sigma >=0, removes the label_categorical_sigma-th variable from the cofactor matrix
void build_sigma_matrix(const cofactor_t *cofactor, size_t matrix_size, int label_categorical_sigma, uint64_t *cat_array, uint32_t *cat_vars_idxs, int drop_first,
        /* out */ float8 *sigma)
{
    // start numerical data:
    // count
    sigma[0] = cofactor->count;

    // sum1
    const float8 *sum1_scalar_array = cscalar_array(cofactor);
    for (size_t i = 0; i < cofactor->num_continuous_vars; i++){
        sigma[i + 1] = sum1_scalar_array[i];
        sigma[(i + 1) * matrix_size] = sum1_scalar_array[i];
    }

    //sum2 full matrix (from half)
    const float8 *sum2_scalar_array = sum1_scalar_array + cofactor->num_continuous_vars;
    for (size_t row = 0; row < cofactor->num_continuous_vars; row++){
        for (size_t col = 0; col < cofactor->num_continuous_vars; col++){
            if (row > col)
                sigma[((row + 1) * matrix_size) + (col + 1)] = sum2_scalar_array[(col * cofactor->num_continuous_vars) - (((col) * (col + 1)) / 2) + row];
            else
                sigma[((row + 1) * matrix_size) + (col + 1)] = sum2_scalar_array[(row * cofactor->num_continuous_vars) - (((row) * (row + 1)) / 2) + col];
        }
    }
    // (numerical_params) * (numerical_params) allocated

    // start relational data:
    cat_vars_idxs[0] = 0;
    size_t skipped_var_categories = 0;
    const char *relation_data = crelation_array(cofactor);
    // count * categorical (group by A, group by B, ...)
    for (size_t i = 0; i < cofactor->num_categorical_vars; i++)
    {
        relation_t *r = (relation_t *) relation_data;
        if (label_categorical_sigma >= 0 && ((size_t)label_categorical_sigma) == i )
        {
            //skip this variable
            skipped_var_categories = (cat_vars_idxs[i + 1] - cat_vars_idxs[i]);
            relation_data += r->sz_struct;
            continue;
        }

        for (size_t j = 0; j < r->num_tuples; j++)
        {
            // search key index
            size_t key_index = find_in_array(r->tuples[j].key, cat_array, cat_vars_idxs[i], cat_vars_idxs[i + 1]);
            if (drop_first && key_index == cat_vars_idxs[i + 1])
                continue;//skip

            assert(key_index < cat_vars_idxs[i + 1]);
            // add to sigma matrix
            key_index += cofactor->num_continuous_vars + 1 - skipped_var_categories;
            sigma[key_index] = r->tuples[j].value;
            sigma[key_index * matrix_size] = r->tuples[j].value;
            sigma[(key_index * matrix_size) + key_index] = r->tuples[j].value;
        }
        relation_data += r->sz_struct;
    }
    // categorical * numerical
    for (size_t numerical = 1; numerical < cofactor->num_continuous_vars + 1; numerical++){
        skipped_var_categories = 0;
        for (size_t categorical = 0; categorical < cofactor->num_categorical_vars; categorical++){
            relation_t *r = (relation_t *) relation_data;
            if (label_categorical_sigma >= 0 && ((size_t)label_categorical_sigma) == categorical ){
                //skip this variable
                skipped_var_categories = (cat_vars_idxs[categorical + 1] - cat_vars_idxs[categorical]);
                relation_data += r->sz_struct;
                continue;
            }

            for (size_t j = 0; j < r->num_tuples; j++)
            {
                //search in the right categorical var
                size_t key_index = find_in_array(r->tuples[j].key, cat_array, cat_vars_idxs[categorical], cat_vars_idxs[categorical + 1]);
                if (drop_first && key_index == cat_vars_idxs[categorical + 1])
                    continue;//skip

                assert(key_index < cat_vars_idxs[categorical + 1]);
                // add to sigma matrix
                key_index += cofactor->num_continuous_vars + 1 - skipped_var_categories;
                sigma[(key_index * matrix_size) + numerical] = r->tuples[j].value;
                sigma[(numerical * matrix_size) + key_index] = r->tuples[j].value;
            }
            relation_data += r->sz_struct;
        }
    }
    size_t skip_sec_var_categories = 0;
    skipped_var_categories = 0;
    // categorical * categorical
    for (size_t curr_cat_var = 0; curr_cat_var < cofactor->num_categorical_vars; curr_cat_var++){
        if (((size_t)label_categorical_sigma) == curr_cat_var) {
            skip_sec_var_categories = (cat_vars_idxs[curr_cat_var + 1] - cat_vars_idxs[curr_cat_var]);
            skipped_var_categories = skip_sec_var_categories;
        }
        else
            skipped_var_categories = 0;

        for (size_t other_cat_var = curr_cat_var + 1; other_cat_var < cofactor->num_categorical_vars; other_cat_var++){
            if(((size_t)label_categorical_sigma) == other_cat_var)
                skipped_var_categories = (cat_vars_idxs[curr_cat_var + 1] - cat_vars_idxs[curr_cat_var]);

            relation_t *r = (relation_t *) relation_data;
            if (label_categorical_sigma >= 0 && (((size_t)label_categorical_sigma) == curr_cat_var || ((size_t)label_categorical_sigma) == other_cat_var))
            {
                //skip this variable
                relation_data += r->sz_struct;
                continue;
            }

            for (size_t j = 0; j < r->num_tuples; j++)
            {
                size_t key_index_curr_var = find_in_array(r->tuples[j].slots[0], cat_array, cat_vars_idxs[curr_cat_var], cat_vars_idxs[curr_cat_var + 1]);
                if (drop_first && key_index_curr_var == cat_vars_idxs[curr_cat_var + 1])
                    continue;//skip

                assert(key_index_curr_var < cat_vars_idxs[curr_cat_var + 1]);

                size_t key_index_other_var = find_in_array(r->tuples[j].slots[1], cat_array, cat_vars_idxs[other_cat_var], cat_vars_idxs[other_cat_var + 1]);
                if (drop_first && key_index_other_var == cat_vars_idxs[other_cat_var + 1])
                    continue;//skip

                assert(key_index_other_var < cat_vars_idxs[other_cat_var + 1]);

                // add to sigma matrix
                key_index_curr_var += cofactor->num_continuous_vars + 1 - skip_sec_var_categories;
                key_index_other_var += cofactor->num_continuous_vars + 1 - skipped_var_categories;

                sigma[(key_index_curr_var * matrix_size) + key_index_other_var] = r->tuples[j].value;
                sigma[(key_index_other_var * matrix_size) + key_index_curr_var] = r->tuples[j].value;
            }
            relation_data += r->sz_struct;
        }
    }
}
