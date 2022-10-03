#ifndef RELATION_H
#define RELATION_H

#include <postgres.h>
#include <assert.h>

typedef struct
{
    union
    {
        uint64 key;
        uint32_t slots[2]; /* composite key can consist of two 4B keys */
    };
    float8 value;
} tuple_t;
static_assert(sizeof(tuple_t) == 16, "size of tuple_t not 16 bytes");

typedef struct
{
    uint32_t sz_struct; /* varlena header */
    uint32_t num_tuples;
    tuple_t tuples[FLEXIBLE_ARRAY_MEMBER];
} relation_t;
static_assert(sizeof(relation_t) == 8, "size of relation_t not 8 bytes");

#define SIZEOF_RELATION(n) (sizeof(relation_t) + n * sizeof(tuple_t))

inline size_t sizeof_relation_t(size_t num_tuples)
{
    return sizeof(relation_t) + num_tuples * sizeof(tuple_t);
}

typedef void (*union_relations_fn_t)(const relation_t *, const relation_t *,
                                     /* out */ relation_t *);

void add_relations(const relation_t *r, const relation_t *s,
                   /* out */ relation_t *out);

void subtract_relations(const relation_t *r, const relation_t *s,
                        /* out */ relation_t *out);

void multiply_relations(const relation_t *r, const relation_t *s,
                        /* out */ relation_t *out);

void scale_relation(const relation_t *r, float8 scale_factor,
                    /* out */ relation_t *out);

#endif // RELATION_H
