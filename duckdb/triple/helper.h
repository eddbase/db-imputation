//
// Created by Massimo Perini on 14/06/2023.
//

#ifndef TEST_HELPER_H
#define TEST_HELPER_H

#include <duckdb.hpp>

namespace Triple {

    void register_functions(duckdb::ClientContext &context, size_t n_con_columns, size_t n_cat_columns);

} // Triple

#endif //TEST_HELPER_H
