//
// Created by Massimo Perini on 21/06/2023.
//

#ifndef TEST_LDA_H
#define TEST_LDA_H

#include <duckdb.hpp>
duckdb::Value lda_train(const duckdb::Value &triple, size_t label, double shrinkage);

#endif //TEST_LDA_H
