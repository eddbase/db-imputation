//
// Created by " " on 30/05/2023.
//

#ifndef DUCKDB_TRIPLE_SUM_H
#define DUCKDB_TRIPLE_SUM_H

#include <vector>
#include <duckdb.hpp>
#include <map>
#include <unordered_map>
#include <boost/functional/hash.hpp>
#include <iostream>
#include <boost/unordered_map.hpp>
#include <boost/container/flat_map.hpp>

//#include <boost/unordered/unordered_flat_map.hpp>

namespace Triple {

    struct AggState {
        int count;
        int num_attributes;
        int cat_attributes;

        //int num_keys_total_categories;

        float *lin_agg;
        float *quadratic_agg;

        //boost::container::flat_map<int, float> *lin_cat;
        boost::container::flat_map<int, std::vector<float>> *quad_num_cat;
        boost::container::flat_map<std::pair<int, int>, float> *quad_cat_cat;
    };


    struct StateFunction {
        template<class STATE>
        static void Initialize(STATE &state) {
            state.count = 0;
            state.num_attributes = 0;
            state.cat_attributes = 0;

            state.lin_agg = nullptr;
            state.quadratic_agg = nullptr;
            state.quad_num_cat = nullptr;
            state.quad_cat_cat = nullptr;
            //state->lin_agg = {};
            //state->quadratic_agg = {};
        }

        template<class STATE>
        static void Destroy(STATE &state, duckdb::AggregateInputData &aggr_input_data) {
            delete[] state.lin_agg;
            delete[] state.quad_cat_cat;
            delete[] state.quad_num_cat;
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

    duckdb::Value sum_triple(const duckdb::Value &triple_1, const duckdb::Value &triple_2);

}

#endif //DUCKDB_TRIPLE_SUM_H
