//
// Created by Massimo Perini on 03/06/2023.
//

#include "Sum_no_lift.h"
#include <duckdb/function/scalar/nested_functions.hpp>

#include "Triple_sum.h"
#include "From_duckdb.h"

#include <iostream>
#include <map>

duckdb::unique_ptr<duckdb::FunctionData>
Triple::SumNoLiftBind(duckdb::ClientContext &context, duckdb::AggregateFunction &function,
                 duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &arguments) {

    //std::cout<<arguments.size()<<"\n";
    //std::cout<<(function.arguments.size())<<"\n";

    child_list_t<LogicalType> struct_children;
    struct_children.emplace_back("N", LogicalType::INTEGER);
    struct_children.emplace_back("lin_agg", LogicalType::LIST(LogicalType::FLOAT));
    struct_children.emplace_back("quad_agg", LogicalType::LIST(LogicalType::FLOAT));

    //categorical structures
    child_list_t<LogicalType> lin_cat;
    lin_cat.emplace_back("key", LogicalType::INTEGER);
    lin_cat.emplace_back("value", LogicalType::FLOAT);

    struct_children.emplace_back("lin_cat", LogicalType::LIST(LogicalType::LIST(LogicalType::STRUCT(lin_cat))));

    child_list_t<LogicalType> quad_num_cat;
    quad_num_cat.emplace_back("key", LogicalType::INTEGER);
    quad_num_cat.emplace_back("value", LogicalType::FLOAT);
    struct_children.emplace_back("quad_num_cat", LogicalType::LIST(LogicalType::LIST(LogicalType::STRUCT(quad_num_cat))));

    child_list_t<LogicalType> quad_cat_cat;
    quad_cat_cat.emplace_back("key1", LogicalType::INTEGER);
    quad_cat_cat.emplace_back("key2", LogicalType::INTEGER);
    quad_cat_cat.emplace_back("value", LogicalType::FLOAT);
    struct_children.emplace_back("quad_cat", LogicalType::LIST(LogicalType::LIST(LogicalType::STRUCT(quad_cat_cat))));

    auto struct_type = LogicalType::STRUCT(struct_children);
    function.return_type = struct_type;
    //set arguments
    function.varargs = LogicalType::ANY;
    return duckdb::make_uniq<duckdb::VariableReturnBindData>(function.return_type);
}


//Updates the state with a new value. E.g, For SUM, this adds the value to the state. Input count: n. of columns
void Triple::SumNoLift(duckdb::Vector inputs[], duckdb::AggregateInputData &aggr_input_data, idx_t cols,
                        duckdb::Vector &state_vector, idx_t count) {
    //count: count of tuples to process
    //inputs: inputs to process

    duckdb::UnifiedVectorFormat sdata;
    state_vector.ToUnifiedFormat(count, sdata);

    auto states = (Triple::AggState **) sdata.data;

    size_t num_cols = 0;
    size_t cat_cols = 0;
    UnifiedVectorFormat input_data[cols];
    for (idx_t j=0;j<cols;j++){
        auto col_type = inputs[j].GetType();
        inputs[j].ToUnifiedFormat(count, input_data[j]);
        if (col_type == LogicalType::FLOAT || col_type == LogicalType::DOUBLE)
            num_cols++;
        else
            cat_cols++;
    }

    //auto &input = inputs[1];
    //RecursiveFlatten(input, count);
    //std::cout<<FlatVector::

    //auto value = (float *)input.GetData();
    //SUM N
    //auto state = states[sdata.sel->get_index(0)];//because of parallelism???

    for (int j = 0; j < count; j++) {
        auto state = states[sdata.sel->get_index(j)];
        state->count += 1;
    }

    //SUM LINEAR AGGREGATES:

    for (idx_t j = 0; j < count; j++) {
        size_t curr_numerical = 0;
        size_t curr_cateogrical = 0;
        auto state = states[sdata.sel->get_index(j)];
        //initialize
        if (state->num_attributes == 0 && state->cat_attributes == 0){
            state->num_attributes = num_cols;
            state->cat_attributes = cat_cols;
            state->lin_agg = new float[num_cols];
            for(idx_t k=0; k<num_cols; k++)
                state->lin_agg[k] = 0;
            state->quadratic_agg = new float[(num_cols*(num_cols+1))/2];
            for(idx_t k=0; k<(num_cols*(num_cols+1))/2; k++)
                state->quadratic_agg[k] = 0;

            state->lin_cat = new std::map<int, float>[cat_cols];
            state->quad_num_cat = new std::map<int, float>[num_cols * cat_cols];
            state->quad_cat_cat = new std::map<std::pair<int, int>, float>[cat_cols * (cat_cols + 1)/2];
        }


        for(idx_t k=0; k<cols; k++) {
            auto col_type = inputs[k].GetType();
            if (col_type == LogicalType::FLOAT || col_type == LogicalType::DOUBLE) {//sum numerical
                state->lin_agg[curr_numerical] += UnifiedVectorFormat::GetData<float>(input_data[k])[input_data[k].sel->get_index(j)];
                curr_numerical++;
            }
            else{//sum categorical
                int in_col_val = UnifiedVectorFormat::GetData<int>(input_data[k])[input_data[k].sel->get_index(j)];
                std::map<int, float> &col_vals = state->lin_cat[curr_cateogrical];
                auto pos = col_vals.find(in_col_val);
                if (pos == col_vals.end())
                    col_vals[in_col_val] = 1;
                else
                    pos->second++;
                curr_cateogrical++;
            }
        }
    }

    //SUM QUADRATIC NUMERICAL AGGREGATES:
    int col_idx = 0;

    for(idx_t j=0; j<cols; j++) {
        auto col_type = inputs[j].GetType();
        if (col_type == LogicalType::FLOAT || col_type == LogicalType::DOUBLE) {//numericals
            const float *first_column_data = UnifiedVectorFormat::GetData<float>(input_data[j]);
            for (idx_t k = j; k < cols; k++) {
                auto col_type = inputs[k].GetType();
                if (col_type == LogicalType::FLOAT || col_type == LogicalType::DOUBLE) {//numerical
                    const float *sec_column_data = UnifiedVectorFormat::GetData<float>(input_data[k]);
                    for (idx_t i = 0; i < count; i++) {
                        auto state = states[sdata.sel->get_index(i)];
                        state->quadratic_agg[col_idx] += first_column_data[input_data[j].sel->get_index(i)] * sec_column_data[input_data[k].sel->get_index(i)];
                    }
                    col_idx++;
                }
            }
        }
    }

    //SUM NUM*CAT
    col_idx = 0;

    for(idx_t j=0; j<cols; j++) {
        auto col_type = inputs[j].GetType();
        if (col_type == LogicalType::FLOAT || col_type == LogicalType::DOUBLE) {//numericals
            const float *first_column_data = UnifiedVectorFormat::GetData<float>(input_data[j]);
            for (idx_t k = 0; k < cols; k++) {
                auto col_type = inputs[k].GetType();
                if (col_type != LogicalType::FLOAT && col_type != LogicalType::DOUBLE) {//categorical
                    const int *sec_column_data = UnifiedVectorFormat::GetData<int>(input_data[k]);
                    for (idx_t i = 0; i < count; i++) {
                        auto state = states[sdata.sel->get_index(i)];
                        std::map<int, float> &vals = state->quad_num_cat[col_idx];
                        int key = sec_column_data[input_data[k].sel->get_index(i)];
                        auto pos = vals.find(key);
                        if (pos == vals.end())
                            vals[key] = first_column_data[input_data[j].sel->get_index(i)];
                        else
                            pos->second += first_column_data[input_data[j].sel->get_index(i)];
                    }
                    col_idx++;
                }
            }
        }
    }

    //SUM CAT*CAT
    col_idx = 0;
    for(idx_t j=0; j<cols; j++) {
        auto col_type = inputs[j].GetType();
        if (col_type != LogicalType::FLOAT && col_type != LogicalType::DOUBLE) {//categorical
            const int *first_column_data = UnifiedVectorFormat::GetData<int>(input_data[j]);
            for (idx_t k = j; k < cols; k++) {
                auto col_type = inputs[k].GetType();
                if (col_type != LogicalType::FLOAT && col_type != LogicalType::DOUBLE) {//categorical
                    const int *sec_column_data = UnifiedVectorFormat::GetData<int>(input_data[k]);
                    for (idx_t i = 0; i < count; i++) {
                        auto state = states[sdata.sel->get_index(i)];
                        std::map<std::pair<int, int>, float> &vals = state->quad_cat_cat[col_idx];
                        auto entry = std::pair<int, int>(first_column_data[input_data[j].sel->get_index(i)], sec_column_data[input_data[k].sel->get_index(i)]);
                        auto pos = vals.find(entry);
                        if (pos == vals.end())
                            vals[entry] = 1;
                        else
                            pos->second += 1;
                    }
                    col_idx++;
                }
            }
        }
    }
}

void
Triple::SumNoLiftCombine(duckdb::Vector &state, duckdb::Vector &combined, duckdb::AggregateInputData &aggr_input_data,
                    idx_t count) {

    duckdb::UnifiedVectorFormat sdata;
    state.ToUnifiedFormat(count, sdata);
    auto states_ptr = (Triple::AggState **) sdata.data;

    auto combined_ptr = duckdb::FlatVector::GetData<Triple::AggState *>(combined);

    for (idx_t i = 0; i < count; i++) {
        auto state = states_ptr[sdata.sel->get_index(i)];
        combined_ptr[i]->count += state->count;
        if (combined_ptr[i]->num_attributes == 0 && combined_ptr[i]->cat_attributes == 0) {//init

            combined_ptr[i]->num_attributes = state->num_attributes;
            combined_ptr[i]->cat_attributes = state->cat_attributes;
            combined_ptr[i]->lin_agg = new float[state->num_attributes];
            for(idx_t k=0; k<state->num_attributes; k++)
                combined_ptr[i]->lin_agg[k] = 0;
            combined_ptr[i]->quadratic_agg = new float[(state->num_attributes*(state->num_attributes+1))/2];
            for(idx_t k=0; k<(state->num_attributes*(state->num_attributes+1))/2; k++)
                combined_ptr[i]->quadratic_agg[k] = 0;

            combined_ptr[i]->lin_cat = new std::map<int, float>[state->cat_attributes];
            combined_ptr[i]->quad_num_cat = new std::map<int, float>[state->num_attributes * state->cat_attributes];
            combined_ptr[i]->quad_cat_cat = new std::map<std::pair<int, int>, float>[state->cat_attributes * (state->cat_attributes + 1)/2];
        }

        //SUM NUMERICAL STATES
        for (int j = 0; j < combined_ptr[i]->num_attributes; j++) {
            combined_ptr[i]->lin_agg[j] += state->lin_agg[j];
        }
        for (int j = 0; j < combined_ptr[i]->num_attributes*(combined_ptr[i]->num_attributes+1)/2; j++) {
            combined_ptr[i]->quadratic_agg[j] += state->quadratic_agg[j];
        }
        //SUM CATEGORICAL STATES
        //linear categorical states
        for (int j = 0; j < combined_ptr[i]->cat_attributes; j++) {
            auto &taget_map = combined_ptr[i]->lin_cat[j];
            for (auto const& state_vals : state->lin_cat[j]){//search in combine states map state values
                auto pos = taget_map.find(state_vals.first);
                if (pos == taget_map.end())
                    taget_map[state_vals.first] = state_vals.second;
                else
                    pos->second += state_vals.second;
            }
        }

        //num*categorical
        for (int j = 0; j < combined_ptr[i]->cat_attributes*combined_ptr[i]->num_attributes; j++) {
            auto &taget_map = combined_ptr[i]->quad_num_cat[j];
            for (auto const& state_vals : state->quad_num_cat[j]){//search in combine states map state values
                auto pos = taget_map.find(state_vals.first);
                if (pos == taget_map.end())
                    taget_map[state_vals.first] = state_vals.second;
                else
                    pos->second += state_vals.second;
            }
        }

        //categorical*categorical
        for (int j = 0; j < combined_ptr[i]->cat_attributes*(combined_ptr[i]->cat_attributes+1)/2; j++) {
            auto &taget_map = combined_ptr[i]->quad_cat_cat[j];
            for (auto const& state_vals : state->quad_cat_cat[j]){//search in combine states map state values
                auto pos = taget_map.find(state_vals.first);
                if (pos == taget_map.end())
                    taget_map[state_vals.first] = state_vals.second;
                else
                    pos->second += state_vals.second;
            }
        }
    }
    //std::cout<<"\nCOMBINE END\n";
}

void Triple::SumNoLiftFinalize(duckdb::Vector &state_vector, duckdb::AggregateInputData &aggr_input_data, duckdb::Vector &result,
                  idx_t count,
                  idx_t offset) {
    //std::cout<<"FINALIZE START count: "<<count<<"offset "<<offset<<std::endl;
    duckdb::UnifiedVectorFormat sdata;
    state_vector.ToUnifiedFormat(count, sdata);
    auto states = (Triple::AggState **) sdata.data;
            D_ASSERT(result.GetType().id() == duckdb::LogicalTypeId::STRUCT);
    auto &children = duckdb::StructVector::GetEntries(result);
    //size_t total_len = ListVector::GetListSize(result);
    //Set N
    Vector &c1 = *(children[0]);//N
    auto input_data = (int32_t *) FlatVector::GetData(c1);

    for (idx_t i=0;i<count; i++) {
        const auto row_id = i + offset;
        input_data[row_id] = states[sdata.sel->get_index(i)]->count;
    }
    //Set List
    Vector &c2 = *(children[1]);
            D_ASSERT(c2.GetType().id() == LogicalTypeId::LIST);
    c2.SetVectorType(VectorType::FLAT_VECTOR);
    auto result_data = FlatVector::GetData<list_entry_t>(c2);
    for (idx_t i = 0; i < count; i++) {
        auto state = states[sdata.sel->get_index(i)];
        const auto row_id = i + offset;
        for (int j = 0; j < state->num_attributes; j++) {
            ListVector::PushBack(c2,
                                 Value(state->lin_agg[j]));//Value::STRUCT({std::make_pair("key", bucket_value), std::make_pair("value", count_value)});
        }
        result_data[row_id].length = state->num_attributes;
        result_data[row_id].offset = result_data[i].length*i;//ListVector::GetListSize(c2);
    }

    //set quadratic attributes
    Vector &c3 = *(children[2]);
            D_ASSERT(c3.GetType().id() == LogicalTypeId::LIST);
    c3.SetVectorType(VectorType::FLAT_VECTOR);
    auto result_data2 = FlatVector::GetData<list_entry_t>(c3);
    for (idx_t i = 0; i < count; i++) {
        auto state = states[sdata.sel->get_index(i)];
        const auto row_id = i + offset;
        for (int j = 0; j < state->num_attributes*(state->num_attributes+1)/2; j++) {
            ListVector::PushBack(c3,
                                 Value(state->quadratic_agg[j]));//Value::STRUCT({std::make_pair("key", bucket_value), std::make_pair("value", count_value)});
        }//Value::Numeric
        result_data2[row_id].length = state->num_attributes*(state->num_attributes+1)/2;
        result_data2[row_id].offset = i * result_data2[i].length;
    }

    //categorical sums
    Vector &c4 = *(children[3]);
            D_ASSERT(c4.GetType().id() == LogicalTypeId::LIST);
    c4.SetVectorType(VectorType::FLAT_VECTOR);
    auto result_data3 = FlatVector::GetData<list_entry_t>(c4);

    for (idx_t i = 0; i < count; i++) {
        auto state = states[sdata.sel->get_index(i)];
        const auto row_id = i + offset;
        for (int j = 0; j < state->cat_attributes; j++) {
            vector<Value> cat_vals = {};
            for (auto const& state_val : state->lin_cat[j]){
                child_list_t<Value> struct_values;
                struct_values.emplace_back("key", Value(state_val.first));
                struct_values.emplace_back("value", Value(state_val.second));
                cat_vals.push_back(duckdb::Value::STRUCT(struct_values));
            }
            ListVector::PushBack(c4, duckdb::Value::LIST(cat_vals));
        }
        result_data3[row_id].length = state->cat_attributes;
        result_data3[row_id].offset = i * result_data3[i].length;
    }


    //categorical num*cat
    Vector &c5 = *(children[4]);
    D_ASSERT(c5.GetType().id() == LogicalTypeId::LIST);
    c5.SetVectorType(VectorType::FLAT_VECTOR);
    auto result_data4 = FlatVector::GetData<list_entry_t>(c5);

    for (idx_t i = 0; i < count; i++) {
        auto state = states[sdata.sel->get_index(i)];
        const auto row_id = i + offset;
        for (int j = 0; j < state->cat_attributes * state->num_attributes; j++) {
            vector<Value> cat_vals = {};
            for (auto const& state_val : state->quad_num_cat[j]){
                child_list_t<Value> struct_values;
                struct_values.emplace_back("key", Value(state_val.first));
                struct_values.emplace_back("value", Value(state_val.second));
                cat_vals.push_back(duckdb::Value::STRUCT(struct_values));
            }
            ListVector::PushBack(c5, duckdb::Value::LIST(cat_vals));
        }
        result_data4[row_id].length = state->cat_attributes * state->num_attributes;
        result_data4[row_id].offset = i * result_data4[i].length;
    }

    //categorical cat*cat
    Vector &c6 = *(children[5]);
    D_ASSERT(c6.GetType().id() == LogicalTypeId::LIST);
    c6.SetVectorType(VectorType::FLAT_VECTOR);
    auto result_data5 = FlatVector::GetData<list_entry_t>(c6);

    for (idx_t i = 0; i < count; i++) {
        auto state = states[sdata.sel->get_index(i)];
        const auto row_id = i + offset;
        for (int j = 0; j < state->cat_attributes * (state->cat_attributes+1)/2; j++) {
            vector<Value> cat_vals = {};
            for (auto const& state_val : state->quad_cat_cat[j]){
                child_list_t<Value> struct_values;
                struct_values.emplace_back("key1", Value(state_val.first.first));
                struct_values.emplace_back("key2", Value(state_val.first.second));
                struct_values.emplace_back("value", Value(state_val.second));
                cat_vals.push_back(duckdb::Value::STRUCT(struct_values));
            }
            ListVector::PushBack(c6, duckdb::Value::LIST(cat_vals));
        }
        result_data5[row_id].length = state->cat_attributes * (state->cat_attributes+1) / 2;
        result_data5[row_id].offset = i * result_data5[i].length;
    }



    //std::cout<<"\nFINALIZE END\n";
}