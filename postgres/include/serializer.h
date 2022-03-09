#ifndef SERIALIZER_H
#define SERIALIZER_H

#include "relation.h"

int read_scalar_array(const char *buf, size_t sz, /* out */ float8 *array);

int write_scalar_array(size_t sz, const float8 *array, /* out */ char *buf);

int size_for_write_scalar_array(size_t sz, const float8 *array);

int read_tuple_array(const char *buf, size_t sz, /* out */ tuple_t *array);

int write_tuple_array(size_t sz, const tuple_t *array, /* out */ char *buf);

int size_for_write_tuple_array(size_t sz, const tuple_t *array);

int read_cofactor_data(const char *buf, size_t sz_scalar_array,
                       size_t sz_relation_array, /* out */ char *data);

int write_cofactor_data(size_t sz_scalar_array, size_t sz_relation_array,
                        const char *data, /* out */ char *buf);

int size_for_write_cofactor_data(size_t sz_scalar_array,
                                 size_t sz_relation_array,
                                 const char *data);

#endif // SERIALIZER_H
