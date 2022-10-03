#include "serializer.h"
#include <stdio.h>

/*****************************************************************************
 * Input/Output functions
 *****************************************************************************/

int read_scalar_array(const char *buf, size_t sz, /* out */ float8 *array)
{
    const char *buf0 = buf;
    int offset;
    sscanf(buf, "[%n", &offset);
    buf += offset;
    for (size_t i = 0; i < sz; i++)
    {
        if (i == 0)
            sscanf(buf, " %lf%n", array + i, &offset);
        else
            sscanf(buf, ", %lf%n", array + i, &offset);
        buf += offset;
    }
    sscanf(buf, " ]%n", &offset);
    buf += offset;
    return buf - buf0;
}

int write_scalar_array(size_t sz, const float8 *array, /* out */ char *buf)
{
    int cursor = sprintf(buf, "[");
    for (size_t i = 0; i < sz; i++)
    {
        if (i == 0)
            cursor += sprintf(buf + cursor, " %lf", array[i]);
        else
            cursor += sprintf(buf + cursor, ", %lf", array[i]);
    }
    cursor += sprintf(buf + cursor, " ]");
    return cursor;
}

int size_for_write_scalar_array(size_t sz, const float8 *array)
{
    int cursor = snprintf(NULL, 0, "[");
    for (size_t i = 0; i < sz; i++)
    {
        if (i == 0)
            cursor += snprintf(NULL, 0, " %lf", array[i]);
        else
            cursor += snprintf(NULL, 0, ", %lf", array[i]);
    }
    cursor += snprintf(NULL, 0, " ]");
    return cursor;
}

int read_tuple_array(const char *buf, size_t sz, /* out */ tuple_t *array)
{
    const char *buf0 = buf;
    int offset;
    sscanf(buf, "{%n", &offset);
    buf += offset;
    for (size_t i = 0; i < sz; i++)
    {
        if (i == 0)
            sscanf(buf, " %lu -> %lf%n", &array[i].key, &array[i].value, &offset);
        else
            sscanf(buf, ", %lu -> %lf%n", &array[i].key, &array[i].value, &offset);
        buf += offset;
    }
    sscanf(buf, " }%n", &offset);
    buf += offset;
    return buf - buf0;
}

int write_tuple_array(size_t sz, const tuple_t *array, /* out */ char *buf)
{
    int cursor = sprintf(buf, "{");
    for (size_t i = 0; i < sz; i++)
    {
        if (i == 0)
            cursor += sprintf(buf + cursor, " %lu -> %lf", array[i].key, array[i].value);
        else
            cursor += sprintf(buf + cursor, ", %lu -> %lf", array[i].key, array[i].value);
    }
    cursor += sprintf(buf + cursor, " }");
    return cursor;
}

int size_for_write_tuple_array(size_t sz, const tuple_t *array)
{
    int cursor = snprintf(NULL, 0, "{");
    for (size_t i = 0; i < sz; i++)
    {
        if (i == 0)
            cursor += snprintf(NULL, 0, " %lu -> %lf", array[i].key, array[i].value);
        else
            cursor += snprintf(NULL, 0, ", %lu -> %lf", array[i].key, array[i].value);
    }
    cursor += snprintf(NULL, 0, " }");
    return cursor;
}

int read_cofactor_data(const char *buf, size_t sz_scalar_array,
                       size_t sz_relation_array, /* out */ char *data)
{
    const char *buf0 = buf;

    int offset = read_scalar_array(buf, sz_scalar_array, (float8 *)data);
    buf += offset;

    sscanf(buf, ", [%n", &offset);
    buf += offset;
    char *relation_data = (char *)(data + sz_scalar_array * sizeof(float8));
    for (size_t i = 0; i < sz_relation_array; i++)
    {
        relation_t *r = (relation_t *)relation_data;
        if (i == 0)
            sscanf(buf, " %u: %n", &r->num_tuples, &offset);
        else
            sscanf(buf, ", %u: %n", &r->num_tuples, &offset);
        buf += offset;

        offset = read_tuple_array(buf, r->num_tuples, r->tuples);
        buf += offset;

        r->sz_struct = sizeof_relation_t(r->num_tuples);
        relation_data += r->sz_struct;
    }
    sscanf(buf, " ]%n", &offset);
    buf += offset;
    return buf - buf0;
}

int write_cofactor_data(size_t sz_scalar_array, size_t sz_relation_array,
                        const char *data, /* out */ char *buf)
{
    int cursor = write_scalar_array(sz_scalar_array, (float8 *)data, buf);
    cursor += sprintf(buf + cursor, ", [");

    char *relation_data = (char *)(data + sz_scalar_array * sizeof(float8));
    for (size_t i = 0; i < sz_relation_array; i++)
    {
        relation_t *r = (relation_t *)relation_data;
        if (i == 0)
            cursor += sprintf(buf + cursor, " %u: ", r->num_tuples);
        else
            cursor += sprintf(buf + cursor, ", %u: ", r->num_tuples);
        cursor += write_tuple_array(r->num_tuples, r->tuples, buf + cursor);

        relation_data += r->sz_struct;
    }
    cursor += sprintf(buf + cursor, " ]");
    return cursor;
}

int size_for_write_cofactor_data(size_t sz_scalar_array,
                                 size_t sz_relation_array, const char *data)
{
    int cursor = size_for_write_scalar_array(sz_scalar_array, (float8 *)data);
    cursor += snprintf(NULL, 0, ", [");

    char *relation_data = (char *)(data + sz_scalar_array * sizeof(float8));
    for (size_t i = 0; i < sz_relation_array; i++)
    {
        relation_t *r = (relation_t *)relation_data;
        if (i == 0)
            cursor += snprintf(NULL, 0, " %u: ", r->num_tuples);
        else
            cursor += snprintf(NULL, 0, ", %u: ", r->num_tuples);
        cursor += size_for_write_tuple_array(r->num_tuples, r->tuples);

        relation_data += r->sz_struct;
    }
    cursor += snprintf(NULL, 0, " ]");
    return cursor;
}
