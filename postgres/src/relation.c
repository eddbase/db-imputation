#include "relation.h"
#include "serializer.h"

#include <catalog/pg_type.h>
#include <fmgr.h>

PG_MODULE_MAGIC;

/*****************************************************************************
 * Input/output functions
 *****************************************************************************/

PG_FUNCTION_INFO_V1(read_relation);

Datum read_relation(PG_FUNCTION_ARGS)
{
    const char *buf = PG_GETARG_CSTRING(0);

    uint32 num_tuples;
    int offset;
    sscanf(buf, "%u, %n", &num_tuples, &offset);

    size_t sz_relation = sizeof_relation_t(num_tuples);

    // allocate data
    relation_t *out = (relation_t *)palloc0(sz_relation);
    SET_VARSIZE(out, sz_relation);

    // set header data
    out->num_tuples = num_tuples;

    read_tuple_array(buf + offset, out->num_tuples, out->tuples);

    PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(write_relation);

Datum write_relation(PG_FUNCTION_ARGS)
{
    relation_t *r = (relation_t *)PG_GETARG_VARLENA_P(0);

    // get buffer size
    size_t bufsz = snprintf(NULL, 0, "%u, ", r->num_tuples);
    bufsz += size_for_write_tuple_array(r->num_tuples, r->tuples);

    // allocate memory
    char *out = (char *)palloc0((bufsz + 1) * sizeof(char));

    // stringify
    int offset = sprintf(out, "%u, ", r->num_tuples);
    write_tuple_array(r->num_tuples, r->tuples, out + offset);

    PG_RETURN_CSTRING(out);
}

/*****************************************************************************
 * Algebraic functions
 *****************************************************************************/

//
// Union relations with compaction
//
inline void add_relations_merge(const relation_t *r, const relation_t *s,
        /* out */ relation_t *out)
{
    out->num_tuples = 0;

    size_t i = 0, j = 0;
    while (i < r->num_tuples && j < s->num_tuples)
    {
        if (r->tuples[i].key == s->tuples[j].key)
        {
            out->tuples[out->num_tuples].key = r->tuples[i].key;
            out->tuples[out->num_tuples].value = r->tuples[i].value + s->tuples[j].value;
            out->num_tuples++;
            i++;
            j++;
        }
        else if (r->tuples[i].key < s->tuples[j].key)
        {
            out->tuples[out->num_tuples] = r->tuples[i];
            out->num_tuples++;
            i++;
        }
        else
        {
            out->tuples[out->num_tuples] = s->tuples[j];
            out->num_tuples++;
            j++;
        }
    }
    while (i < r->num_tuples)
    {
        out->tuples[out->num_tuples] = r->tuples[i];
        out->num_tuples++;
        i++;
    }
    while (j < s->num_tuples)
    {
        out->tuples[out->num_tuples] = s->tuples[j];
        out->num_tuples++;
        j++;
    }
    out->sz_struct = sizeof_relation_t(out->num_tuples);
}

inline void add_relations_linear(const relation_t *r, const relation_t *s,
        /* out */ relation_t *out)
{
    memcpy((void *) out, (void *) r, r->sz_struct);

    for (size_t i = 0; i < s->num_tuples; i++)
    {
        bool found = false;
        for (size_t j = 0; !found && j < r->num_tuples; j++)
        {
            if (out->tuples[j].key == s->tuples[i].key)
            {
                out->tuples[j].value += s->tuples[i].value;
                found = true;
            }
        }
        if (!found)
        {
            out->tuples[out->num_tuples] = s->tuples[i];
            out->num_tuples++;
        }
    }
    out->sz_struct = sizeof_relation_t(out->num_tuples);
}

inline void add_relations_singleton(const relation_t *r, tuple_t s_tuple,
        /* out */ relation_t *out)
{
    out->num_tuples = r->num_tuples;

    bool found = false;
    for (size_t i = 0; i < r->num_tuples; i++)
    {
        out->tuples[i] = r->tuples[i];

        if (!found && out->tuples[i].key == s_tuple.key) {
            out->tuples[i].value += s_tuple.value;
            found = true;
        }
    }
    if (!found)
    {
        out->tuples[out->num_tuples] = s_tuple;
        out->num_tuples++;
    }
    out->sz_struct = sizeof_relation_t(out->num_tuples);
}

void add_relations(const relation_t *r, const relation_t *s,
        /* out */ relation_t *out)
{
    if (r->num_tuples >= s->num_tuples)
    {
        add_relations_linear(r, s, out);
    }
    else
    {
        add_relations_linear(s, r, out);
    }
}

//
// Subtract relations with compaction; equivalent to add_relations(r, -s, out)
//
void subtract_relations(const relation_t *r, const relation_t *s,
        /* out */ relation_t *out)
{
    out->num_tuples = r->num_tuples;
    for (size_t i = 0; i < r->num_tuples; i++)
    {
        out->tuples[i] = r->tuples[i];
    }
    for (size_t i = 0; i < s->num_tuples; i++)
    {
        bool found = false;
        for (size_t j = 0; !found && j < r->num_tuples; j++)
        {
            if (out->tuples[j].key == s->tuples[i].key)
            {
                out->tuples[j].value -= s->tuples[i].value;
                found = true;
            }
        }
        if (!found)
        {
            out->tuples[out->num_tuples].key = s->tuples[i].key;
            out->tuples[out->num_tuples].value = -s->tuples[i].value;
            out->num_tuples++;
        }
    }
    out->sz_struct = sizeof_relation_t(out->num_tuples);
}

void multiply_relations(const relation_t *r, const relation_t *s,
        /* out */ relation_t *out)
{
    out->num_tuples = r->num_tuples * s->num_tuples;
    out->sz_struct = sizeof_relation_t(out->num_tuples);
    tuple_t *tuple = out->tuples;
    for (size_t i = 0; i < r->num_tuples; i++)
    {
        for (size_t j = 0; j < s->num_tuples; j++)
        {
            tuple->slots[0] = r->tuples[i].slots[0];
            tuple->slots[1] = s->tuples[j].slots[0];
            tuple->value = r->tuples[i].value * s->tuples[j].value;
            tuple++;
        }
    }
}

void scale_relation(const relation_t *r, float8 scale_factor,
        /* out */ relation_t *out)
{
    out->sz_struct = r->sz_struct;
    out->num_tuples = r->num_tuples;
    for (size_t i = 0; i < r->num_tuples; i++)
    {
        out->tuples[i].key = r->tuples[i].key;
        out->tuples[i].value = scale_factor * r->tuples[i].value;
    }
}

PG_FUNCTION_INFO_V1(pg_add_relations);

Datum pg_add_relations(PG_FUNCTION_ARGS)
{
    relation_t *a = (relation_t *)PG_GETARG_VARLENA_P(0);
    relation_t *b = (relation_t *)PG_GETARG_VARLENA_P(1);

    // allocate data
    size_t sz = sizeof_relation_t(a->num_tuples + b->num_tuples);
    relation_t *out = (relation_t *)palloc0(sz);

    add_relations(a, b, out);
    SET_VARSIZE(out, sz);

    PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(pg_subtract_relations);

Datum pg_subtract_relations(PG_FUNCTION_ARGS)
{
    relation_t *a = (relation_t *)PG_GETARG_VARLENA_P(0);
    relation_t *b = (relation_t *)PG_GETARG_VARLENA_P(1);

    // allocate data
    size_t sz = sizeof_relation_t(a->num_tuples + b->num_tuples);
    relation_t *out = (relation_t *)palloc0(sz);

    subtract_relations(a, b, out);
    SET_VARSIZE(out, sz);

    PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(pg_multiply_relations);

Datum pg_multiply_relations(PG_FUNCTION_ARGS)
{
    relation_t *a = (relation_t *)PG_GETARG_VARLENA_P(0);
    relation_t *b = (relation_t *)PG_GETARG_VARLENA_P(1);

    // allocate data
    size_t sz = sizeof_relation_t(a->num_tuples * b->num_tuples);
    relation_t *out = (relation_t *)palloc0(sz);

    multiply_relations(a, b, out);
    SET_VARSIZE(out, sz);

    PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(pg_scale_relation);

Datum pg_scale_relation(PG_FUNCTION_ARGS)
{
    relation_t *a = (relation_t *)PG_GETARG_VARLENA_P(0);
    float8 b = PG_GETARG_FLOAT8(1);

    // allocate data
    size_t sz = sizeof_relation_t(a->num_tuples);
    relation_t *out = (relation_t *)palloc0(sz);

    scale_relation(a, b, out);
    SET_VARSIZE(out, sz);

    PG_RETURN_POINTER(out);
}

/*****************************************************************************
 * Lift functions
 *****************************************************************************/

PG_FUNCTION_INFO_V1(lift_to_relation);

Datum lift_to_relation(PG_FUNCTION_ARGS)
{
    size_t sz = SIZEOF_RELATION(1);
    relation_t *out = (relation_t *)palloc0(sz);
    SET_VARSIZE(out, sz);

    out->num_tuples = 1;
    out->tuples[0].key = PG_GETARG_UINT32(0);
    out->tuples[0].value = 1.0;

    PG_RETURN_POINTER(out);
}
