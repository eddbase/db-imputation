//
// Created by Massimo Perini on 30/05/2023.
//

#include "Triple_sum.h"
#include "From_duckdb.h"
#include <duckdb/function/scalar/nested_functions.hpp>

#include <iostream>
#include <memory>
namespace Triple {

    duckdb::unique_ptr<duckdb::FunctionData>
    ListBindFunction(duckdb::ClientContext &context, duckdb::AggregateFunction &function,
                     duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &arguments) {

        D_ASSERT(arguments.size() == 1);
        D_ASSERT(function.arguments.size() == 1);

        //std::cout<<"SUM ARGS: "<<arguments[0]->ToString();
        //std::cout<<"SUM ARGS 2: "<<function.arguments[0].ToString();


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
        return duckdb::make_uniq<duckdb::VariableReturnBindData>(function.return_type);
    }


//Updates the state with a new value. E.g, For SUM, this adds the value to the state
    void ListUpdateFunction(duckdb::Vector inputs[], duckdb::AggregateInputData &aggr_input_data, idx_t input_count,
                            duckdb::Vector &state_vector, idx_t count) {
                D_ASSERT(input_count == 1);
        //count: count of tuples to process
        //inputs: inputs to process

        //std::cout << "UPDATE START" << "\nINPUTS: " << inputs->ToString() << "\nINPUT COUNT : "
        //          << input_count << "\nSTATE VECTOR: " << state_vector.ToString() << "\nCOUNT: " << count<<std::endl;

        duckdb::UnifiedVectorFormat sdata;
        state_vector.ToUnifiedFormat(count, sdata);

        auto states = (AggState **) sdata.data;

        auto &input = inputs[0];
        duckdb::RecursiveFlatten(input, count);//not sure it's needed
        D_ASSERT(input.GetVectorType() == VectorType::FLAT_VECTOR);

        //SUM N
        auto &children = StructVector::GetEntries(input);
        Vector &c1 = *children[0];//this should be the vector of integers....

        UnifiedVectorFormat input_data;
        c1.ToUnifiedFormat(count, input_data);
        auto c1_u = UnifiedVectorFormat::GetData<int32_t>(input_data);

        //auto input_data = (int32_t *) FlatVector::GetData(c1);
        //auto state = states[sdata.sel->get_index(0)];//because of parallelism???

        for (int j = 0; j < count; j++) {
            auto state = states[sdata.sel->get_index(j)];
            state->count += c1_u[input_data.sel->get_index(j)];
        }
        //auto N =  input_data;

        //SUM LINEAR AGGREGATES:

        Vector &c2 = *children[1];//this should be the linear aggregate....
        UnifiedVectorFormat input_data_c2;
        duckdb::ListVector::GetEntry(c2).ToUnifiedFormat(count, input_data_c2);
        auto list_entries = UnifiedVectorFormat::GetData<float>(input_data_c2);

        auto num_attr_size = ListVector::GetListSize(c2);
        auto cat_attr_size = ListVector::GetListSize(*children[3]);

        //auto list_entries = (float *) ListVector::GetEntry(c2).GetData();//entries are float
        int num_attr = num_attr_size / count;//cols = total values / num. rows
        int cat_attr = cat_attr_size / count;

        for (idx_t j = 0; j < count; j++) {
            auto state = states[sdata.sel->get_index(j)];

            if (state->num_attributes == 0 && state->cat_attributes == 0){//initialize
                //state->attributes = cols;todo fix this
                state->num_attributes = num_attr;
                state->cat_attributes = cat_attr;
                state->lin_agg = new float[num_attr];
                for(idx_t k=0; k<num_attr; k++)
                    state->lin_agg[k] = 0;
                state->quadratic_agg = new float[(num_attr*(num_attr+1))/2];
                for(idx_t k=0; k<(num_attr*(num_attr+1))/2; k++)
                    state->quadratic_agg[k] = 0;
                state->lin_cat = new std::map<int, float>[cat_attr];
                state->quad_num_cat = new std::map<int, float>[cat_attr * cat_attr];
                state->quad_cat_cat = new std::map<std::pair<int, int>, float>[cat_attr * (cat_attr + 1)/2];
            }

            for(idx_t k=0; k<num_attr; k++) {
                state->lin_agg[k] += list_entries[input_data_c2.sel->get_index(k + (j * num_attr))];
            }
        }

        //SUM QUADRATIC AGGREGATES:
        Vector &c3 = *children[2];//this should be the linear aggregate....
        UnifiedVectorFormat input_data_c3;
        duckdb::ListVector::GetEntry(c3).ToUnifiedFormat(count, input_data_c3);
        auto list_entries_2 = UnifiedVectorFormat::GetData<float>(input_data_c3);

        int cols2 = ((num_attr * (num_attr + 1)) / 2);//quadratic aggregates columns

        for (idx_t j = 0; j < count; j++) {
            auto state = states[sdata.sel->get_index(j)];
            for(idx_t k=0; k<cols2; k++)
                state->quadratic_agg[k] += list_entries_2[input_data_c3.sel->get_index(k + (j*cols2))];
        }

        //sum cat lin. data

        //Vector v_cat_lin = duckdb::ListVector::GetEntry(*children[3]);

        Vector &c4 = ListVector::GetEntry(*children[3]);//this should be the linear aggregate....
        //UnifiedVectorFormat input_data_c4;
        //duckdb::ListVector::GetEntry(c4).ToUnifiedFormat(count, input_data_c4);
        //auto list_entries_2 = UnifiedVectorFormat::GetData<float>(input_data_c3);
        //UnifiedVectorFormat::Get

        //todo also for size_t

        for (idx_t j = 0; j < count; j++) {
            auto state = states[sdata.sel->get_index(j)];
            for(idx_t k=0; k<cat_attr; k++){
                const vector<duckdb::Value> list_of_struct = duckdb::ListValue::GetChildren(c4.GetValue(k + (j*cat_attr)));//[[{a,1},{b,1}]]
                std::map<int, float> &col_vals_state = state->lin_cat[k];
                for (size_t item = 0; item < list_of_struct.size(); item++){//for each column values
                    const vector<Value> &v_struct= duckdb::StructValue::GetChildren(list_of_struct[item]);
                    int key = v_struct[0].GetValue<int>();
                    auto pos = col_vals_state.find(key);
                    if (pos == col_vals_state.end())
                        col_vals_state[key] = v_struct[1].GetValue<float>();
                    else
                        pos->second += v_struct[1].GetValue<float>();
                }
            }
        }

        //set num*cat
        Vector &c5 = ListVector::GetEntry(*children[4]);

        for (idx_t j = 0; j < count; j++) {
            auto state = states[sdata.sel->get_index(j)];
            for(idx_t k=0; k<cat_attr * num_attr; k++){
                const vector<duckdb::Value> children = duckdb::ListValue::GetChildren(c5.GetValue(k + (j*(cat_attr * num_attr))));
                std::map<int, float> &col_vals = state->quad_num_cat[k];
                for (size_t item = 0; item < children.size(); item++){
                    const vector<Value> &struct_children = duckdb::StructValue::GetChildren(children[item]);
                    int key = struct_children[0].GetValue<int>();
                    auto pos = col_vals.find(key);
                    if (pos == col_vals.end())
                        col_vals[key] = struct_children[1].GetValue<float>();
                    else
                        pos->second += struct_children[1].GetValue<float>();
                }
            }
        }

        Vector &c6 = ListVector::GetEntry(*children[5]);

        for (idx_t j = 0; j < count; j++) {
            auto state = states[sdata.sel->get_index(j)];
            for(idx_t k=0; k<(cat_attr * (cat_attr+1))/2; k++){
                const vector<duckdb::Value> children = duckdb::ListValue::GetChildren(c6.GetValue(k + (j*(cat_attr * (cat_attr+1))/2)));
                std::map<std::pair<int, int>, float> &col_vals = state->quad_cat_cat[k];
                for (size_t item = 0; item < children.size(); item++){
                    const vector<Value> &struct_children = duckdb::StructValue::GetChildren(children[item]);
                    std::pair<int, int> key = std::pair<int, int>(struct_children[0].GetValue<int>(), struct_children[1].GetValue<int>());
                    auto pos = col_vals.find(key);
                    if (pos == col_vals.end())
                        col_vals[key] = struct_children[2].GetValue<float>();
                    else
                        pos->second += struct_children[2].GetValue<float>();
                }
            }
        }
    }

    void
    ListCombineFunction(duckdb::Vector &state, duckdb::Vector &combined, duckdb::AggregateInputData &aggr_input_data,
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

    }

    void ListFinalize(duckdb::Vector &state_vector, duckdb::AggregateInputData &aggr_input_data, duckdb::Vector &result,
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

    }

    vector<Value> sum_list_of_structs(const vector<Value> &v1, const vector<Value> &v2){
        std::map<int, float> content;
        vector<Value> col_cat_lin = {};
        for(size_t k=0; k<v1.size(); k++){
            //extract struct
            auto children = duckdb::StructValue::GetChildren(v1[k]);//vector of pointers to childrens
            content[children[0].GetValue<int>()] = children[1].GetValue<float>();
        }

        for(size_t k=0; k<v2.size(); k++){
            //extract struct
            auto children = duckdb::StructValue::GetChildren(v2[k]);//vector of pointers to childrens
            auto pos = content.find(children[0].GetValue<int>());
            if (pos == content.end())
                content[children[0].GetValue<int>()] = children[1].GetValue<float>();
            else
                pos->second += children[1].GetValue<float>();
        }
        for (auto const& value : content){
            child_list_t<Value> struct_values;
            struct_values.emplace_back("key", Value(value.first));
            struct_values.emplace_back("value", Value(value.second));
            col_cat_lin.push_back(duckdb::Value::STRUCT(struct_values));
        }
        return col_cat_lin;
    }

    vector<Value> sum_list_of_structs_key2(const vector<Value> &v1, const vector<Value> &v2){
        std::map<std::pair<int, int>, float> content;
        vector<Value> col_cat_lin = {};
        for(size_t k=0; k<v1.size(); k++){
            //extract struct
            auto children = duckdb::StructValue::GetChildren(v1[k]);//vector of pointers to childrens
            content[std::pair<int, int>(children[0].GetValue<int>(), children[1].GetValue<int>())] = children[2].GetValue<float>();
        }

        for(size_t k=0; k<v2.size(); k++){
            //extract struct
            auto children = duckdb::StructValue::GetChildren(v2[k]);//vector of pointers to childrens
            auto key = std::pair<int, int>(children[0].GetValue<int>(), children[1].GetValue<int>());
            auto pos = content.find(key);
            if (pos == content.end())
                content[key] = children[2].GetValue<float>();
            else
                pos->second += children[2].GetValue<float>();
        }
        for (auto const& value : content){
            child_list_t<Value> struct_values;
            struct_values.emplace_back("key1", Value(value.first.first));
            struct_values.emplace_back("key2", Value(value.first.second));
            struct_values.emplace_back("value", Value(value.second));
            col_cat_lin.push_back(duckdb::Value::STRUCT(struct_values));
        }
        return col_cat_lin;
    }

    duckdb::Value sum_triple(const duckdb::Value &triple_1, const duckdb::Value &triple_2){

        auto first_triple_children = duckdb::StructValue::GetChildren(triple_1);//vector of pointers to childrens
        auto sec_triple_children = duckdb::StructValue::GetChildren(triple_2);

        auto N_1 = first_triple_children[0].GetValue<int>();;
        auto N_2 = sec_triple_children[0].GetValue<int>();

        child_list_t<Value> struct_values;
        struct_values.emplace_back("N", Value(N_1 + N_2));

        const vector<Value> &linear_1 = duckdb::ListValue::GetChildren(first_triple_children[1]);
        const vector<Value> &linear_2 = duckdb::ListValue::GetChildren(sec_triple_children[1]);
        vector<Value> lin = {};
        for(idx_t i=0;i<linear_1.size();i++)
            lin.push_back(Value(linear_1[i].GetValue<float>() + linear_2[i].GetValue<float>()));

        struct_values.emplace_back("lin_num", duckdb::Value::LIST(lin));

        const vector<Value> &quad_1 = duckdb::ListValue::GetChildren(first_triple_children[2]);
        const vector<Value> &quad_2 = duckdb::ListValue::GetChildren(sec_triple_children[2]);
        vector<Value> quad = {};
        for(idx_t i=0;i<quad_1.size();i++)
            quad.push_back(Value(quad_1[i].GetValue<float>() + quad_2[i].GetValue<float>()));

        struct_values.emplace_back("quad_num", duckdb::Value::LIST(quad));

        //categorical linear
        const vector<Value> &cat_linear_1 = duckdb::ListValue::GetChildren(first_triple_children[3]);
        const vector<Value> &cat_linear_2 = duckdb::ListValue::GetChildren(sec_triple_children[3]);
        vector<Value> cat_lin = {};
        for(idx_t i=0;i<linear_1.size();i++) {//for each cat. column copy into map
            const vector<Value> &pairs_cat_col_1 = duckdb::ListValue::GetChildren(cat_linear_1[i]);
            const vector<Value> &pairs_cat_col_2 = duckdb::ListValue::GetChildren(cat_linear_2[i]);
            cat_lin.push_back(duckdb::Value::LIST(sum_list_of_structs(pairs_cat_col_1, pairs_cat_col_2)));
        }
        struct_values.emplace_back("lin_cat", duckdb::Value::LIST(cat_lin));

        //num*cat

        const vector<Value> &num_cat_1 = duckdb::ListValue::GetChildren(first_triple_children[4]);
        const vector<Value> &num_cat_2 = duckdb::ListValue::GetChildren(sec_triple_children[4]);
        vector<Value> num_cat_res = {};
        for(idx_t i=0;i<linear_1.size();i++) {//for each cat. column copy into map
            const vector<Value> &pairs_cat_col_1 = duckdb::ListValue::GetChildren(num_cat_1[i]);
            const vector<Value> &pairs_cat_col_2 = duckdb::ListValue::GetChildren(num_cat_2[i]);
            num_cat_res.push_back(duckdb::Value::LIST(sum_list_of_structs(pairs_cat_col_1, pairs_cat_col_2)));
        }
        struct_values.emplace_back("quad_num_cat", duckdb::Value::LIST(num_cat_res));

        //cat*cat

        const vector<Value> &cat_cat_1 = duckdb::ListValue::GetChildren(first_triple_children[5]);
        const vector<Value> &cat_cat_2 = duckdb::ListValue::GetChildren(sec_triple_children[5]);
        vector<Value> cat_cat_res = {};
        for(idx_t i=0;i<linear_1.size();i++) {//for each cat. column copy into map
            const vector<Value> &pairs_cat_col_1 = duckdb::ListValue::GetChildren(cat_cat_1[i]);
            const vector<Value> &pairs_cat_col_2 = duckdb::ListValue::GetChildren(cat_cat_2[i]);
            cat_cat_res.push_back(duckdb::Value::LIST(sum_list_of_structs_key2(pairs_cat_col_1, pairs_cat_col_2)));
        }
        struct_values.emplace_back("quad_cat", duckdb::Value::LIST(cat_cat_res));

        auto ret = duckdb::Value::STRUCT(struct_values);
        return ret;
    }

}