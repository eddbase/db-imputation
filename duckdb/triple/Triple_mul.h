//
// Created by Massimo Perini on 30/05/2023.
//

#include "../lib/libduckdb/src/duckdb.hpp"

#ifndef DUCKDB_TRIPLE_MUL_H
#define DUCKDB_TRIPLE_MUL_H

namespace Triple{
    void StructPackFunction(duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result);
    duckdb::unique_ptr<duckdb::FunctionData> MultiplyBind(duckdb::ClientContext &context, duckdb::ScalarFunction &function,
                                                          duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &arguments);
    duckdb::unique_ptr<duckdb::BaseStatistics> StructPackStats(duckdb::ClientContext &context, duckdb::FunctionStatisticsInput &input);

}


#endif //DUCKDB_TRIPLE_MUL_H
