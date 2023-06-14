//
// Created by Massimo Perini on 03/06/2023.
//

#ifndef DUCKDB_SUM_NO_LIFT_H
#define DUCKDB_SUM_NO_LIFT_H


#include <vector>
#include <duckdb.hpp>

namespace Triple {

    duckdb::unique_ptr<duckdb::FunctionData>
    SumNoLiftBind(duckdb::ClientContext &context, duckdb::AggregateFunction &function,
                     duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &arguments);

    void SumNoLift(duckdb::Vector inputs[], duckdb::AggregateInputData &aggr_input_data, idx_t input_count,
                            duckdb::Vector &state_vector, idx_t count);

    void
    SumNoLiftCombine(duckdb::Vector &state, duckdb::Vector &combined, duckdb::AggregateInputData &aggr_input_data,
                        idx_t count);

    void SumNoLiftFinalize(duckdb::Vector &state_vector, duckdb::AggregateInputData &aggr_input_data, duckdb::Vector &result,
                      idx_t count,
                      idx_t offset);

}


#endif //DUCKDB_SUM_NO_LIFT_H
