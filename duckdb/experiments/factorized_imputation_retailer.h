//
// Created by Massimo Perini on 11/07/2023.
//

#ifndef TEST_FACTORIZED_IMPUTATION_RETAILER_H
#define TEST_FACTORIZED_IMPUTATION_RETAILER_H

#include <duckdb.hpp>

void run_flight_partition_factorized_retailer(const std::string &path, const std::string &fact_table_name,
                                              size_t mice_iters);
#endif //TEST_FACTORIZED_IMPUTATION_RETAILER_H
