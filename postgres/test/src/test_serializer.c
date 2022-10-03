#include "cofactor.h"
#include "relation.h"
#include "serializer.h"

#include <stdio.h>

// #define printf printf
// #define snprintf snprintf
// #define sprintf sprintf


void test_scalar_array();
void test_tuple_array();
void test_cofactor_data();

void test_scalar_array()
{
    size_t sz_scalar_arr = 5;
    float8 *scalar_arr = malloc(sz_scalar_arr * sizeof(float8));
    scalar_arr[0] = 32.34;
    scalar_arr[1] = 11.1;
    scalar_arr[2] = -231;

    int sz_buf = size_for_write_scalar_array(sz_scalar_arr, scalar_arr);
    char *buf = malloc((sz_buf + 1) * sizeof(char));
    write_scalar_array(sz_scalar_arr, scalar_arr, buf);
    buf[sz_buf] = 0;
    printf("[scalars] serialized: %s\n", buf);

    memset(scalar_arr, 0, sz_scalar_arr * sizeof(float8));

    read_scalar_array(buf, sz_scalar_arr, scalar_arr);

    printf("[scalars] deserialized: ");
    for (size_t i = 0; i < sz_scalar_arr; i++)
    {
        printf("%f ", scalar_arr[i]);
    }
    printf("\n");

    free(scalar_arr);
    free(buf);
}

void test_tuple_array()
{
    size_t sz_tuple_arr = 5;
    tuple_t *tuple_arr = malloc(sz_tuple_arr * sizeof(tuple_t));
    tuple_arr[0] = (tuple_t){1, 32.34};
    tuple_arr[1] = (tuple_t){2, 11.1};
    tuple_arr[2] = (tuple_t){4, -231};

    int sz_buf = size_for_write_tuple_array(sz_tuple_arr, tuple_arr);
    char *buf = malloc((sz_buf + 1) * sizeof(char));
    write_tuple_array(sz_tuple_arr, tuple_arr, buf);
    buf[sz_buf] = 0;
    printf("[tuples] serialized: %s\n", buf);

    memset(tuple_arr, 0, sz_tuple_arr * sizeof(tuple_t));

    read_tuple_array(buf, sz_tuple_arr, tuple_arr);

    printf("[tuples] deserialized: ");
    for (size_t i = 0; i < sz_tuple_arr; i++)
    {
        printf("%lu -> %lf ", tuple_arr[i].key, tuple_arr[i].value);
    }
    printf("\n");

    free(tuple_arr);
    free(buf);
}

void test_cofactor_data()
{
    size_t sz_scalar_array = 5;
    size_t sz_relation_array = 2;
    size_t sz_tuple_array1 = 2;
    size_t sz_tuple_array2 = 4;
    size_t sz_data =
        sizeof(cofactor_t) + sz_scalar_array * sizeof(float8) +
        sizeof(relation_t) + sz_tuple_array1 * sizeof(tuple_t) +
        sizeof(relation_t) + sz_tuple_array2 * sizeof(tuple_t);

    char *data = malloc(sz_data);
    float8 *scalar_arr = (float8 *)data;
    scalar_arr[0] = 32.34;
    scalar_arr[1] = 11.1;
    scalar_arr[2] = -231;

    char *relation_data = (char *)(data + sz_scalar_array * sizeof(float8));
    relation_t *r1 = (relation_t *)relation_data;
    r1->sz_struct = sizeof(relation_t) + sz_tuple_array1 * sizeof(tuple_t);
    r1->num_tuples = sz_tuple_array1;
    r1->tuples[0] = (tuple_t){1, 32.34};
    r1->tuples[1] = (tuple_t){2, 11.1};

    relation_t *r2 = (relation_t *)(relation_data + r1->sz_struct);
    r2->sz_struct = sizeof(relation_t) + sz_tuple_array2 * sizeof(tuple_t);
    r2->num_tuples = sz_tuple_array2;
    r2->tuples[0] = (tuple_t){32, 9.3};
    r2->tuples[1] = (tuple_t){21, 4.993};
    r2->tuples[2] = (tuple_t){65, 3.0};
    r2->tuples[3] = (tuple_t){245, 1.2};

    int sz_buf = size_for_write_cofactor_data(sz_scalar_array, sz_relation_array, data);
    char *buf = malloc((sz_buf + 1) * sizeof(char));
    write_cofactor_data(sz_scalar_array, sz_relation_array, data, buf);
    buf[sz_buf] = 0;
    printf("[cofactor data] serialized: %s\n", buf);

    memset(data, 0, sz_data);

    read_cofactor_data(buf, sz_scalar_array, sz_relation_array, data);

    printf("[cofactor data] deserialized: ");

    printf("[ ");
    for (size_t i = 0; i < sz_scalar_array; i++)
    {
        printf("%lf ", scalar_arr[i]);
    }
    printf("], [ %u: { ", r1->num_tuples);
    for (size_t i = 0; i < r1->num_tuples; i++)
    {
        printf("%lu -> %lf ", r1->tuples[i].key, r1->tuples[i].value);
    }
    printf(" }, %u: { ", r2->num_tuples);
    for (size_t i = 0; i < r2->num_tuples; i++)
    {
        printf("%lu -> %lf ", r2->tuples[i].key, r2->tuples[i].value);
    }
    printf("} ]\n");

    free(data);
    free(buf);
}

int main(int argc, char **argv)
{
    test_scalar_array();
    test_tuple_array();
    test_cofactor_data();
    return 0;
}
