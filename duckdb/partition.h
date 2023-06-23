//
// Created by Massimo Perini on 14/06/2023.
//

#ifndef TEST_PARTITION_H
#define TEST_PARTITION_H

#include <string>
#include <duckdb.hpp>


void partition(const std::string &table_name, const std::vector<std::string> con_columns, const std::vector<std::string> con_columns_nulls,
               const std::vector<std::string> &cat_columns, const std::vector<std::string> cat_columns_nulls, duckdb::Connection &con);
void query_categorical(const std::vector<std::string> &cat_columns, duckdb::Connection &con, size_t label, std::string &cat_columns_query, std::string &predict_column_query);
#endif //TEST_PARTITION_H
