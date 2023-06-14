//
// Created by Massimo Perini on 30/05/2023.
//

#ifndef DUCKDB_TRIPLE_SUM_H
#define DUCKDB_TRIPLE_SUM_H

#include <vector>
#include <duckdb.hpp>

namespace Triple {
    struct AggState {
        int count;
        int attributes;
        float *lin_agg;
        float *quadratic_agg;
    };


    struct StateFunction {
        template<class STATE>
        static void Initialize(STATE &state) {
            state.count = 0;
            state.attributes = 0;
            state.lin_agg = nullptr;
            state.quadratic_agg = nullptr;
            //state->lin_agg = {};
            //state->quadratic_agg = {};
        }

        template<class STATE>
        static void Destroy(STATE &state, duckdb::AggregateInputData &aggr_input_data) {
            delete[] state.lin_agg;
            delete[] state.quadratic_agg;
            //state->lin_agg.clear();
            //state->quadratic_agg.clear();
        }

        static bool IgnoreNull() {
            return false;
        }
    };

    duckdb::unique_ptr<duckdb::FunctionData>
    ListBindFunction(duckdb::ClientContext &context, duckdb::AggregateFunction &function,
                     duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &arguments);

    void ListUpdateFunction(duckdb::Vector inputs[], duckdb::AggregateInputData &aggr_input_data, idx_t input_count,
                            duckdb::Vector &state_vector, idx_t count);

    void
    ListCombineFunction(duckdb::Vector &state, duckdb::Vector &combined, duckdb::AggregateInputData &aggr_input_data,
                        idx_t count);

    void ListFinalize(duckdb::Vector &state_vector, duckdb::AggregateInputData &aggr_input_data, duckdb::Vector &result,
                      idx_t count,
                      idx_t offset);

}

#endif //DUCKDB_TRIPLE_SUM_H
