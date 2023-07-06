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
        int cat_attr = cat_attr_size;
        if (cat_attr > 0)
            cat_attr = duckdb::ListVector::GetData(*children[3])[0].length;


        for (idx_t j = 0; j < count; j++) {
            auto state = states[sdata.sel->get_index(j)];

            if (state->lin_agg == nullptr && state->lin_cat == nullptr){//initialize
                state->num_attributes = num_attr;
                state->cat_attributes = cat_attr;
                if(num_attr > 0) {
                    state->lin_agg = new float[num_attr + ((num_attr * (num_attr + 1)) / 2)];
                    state->quadratic_agg = &(state->lin_agg[num_attr]);
                    for(idx_t k=0; k<num_attr; k++)
                        state->lin_agg[k] = 0;

                    for(idx_t k=0; k<(num_attr*(num_attr+1))/2; k++)
                        state->quadratic_agg[k] = 0;
                }
                if(cat_attr > 0) {
                    state->lin_cat = new boost::container::flat_map<int, float>[cat_attr + (num_attr * cat_attr)];
                    if (num_attr > 0)
                        state->quad_num_cat = &(state->lin_cat[cat_attr]);

                    state->quad_cat_cat = new boost::container::flat_map<std::pair<int, int>, float>[
                    cat_attr * (cat_attr + 1) / 2];
                }
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

        int *cat_set_val_key = nullptr;
        float *cat_set_val_val = nullptr;
        list_entry_t *sublist_metadata = nullptr;

        if(cat_attr > 0){
            Vector v_cat_lin_1 = duckdb::ListVector::GetEntry(*children[3]);
            sublist_metadata = duckdb::ListVector::GetData(v_cat_lin_1);
            Vector v_cat_lin_1_values = duckdb::ListVector::GetEntry(v_cat_lin_1);
            vector<unique_ptr<duckdb::Vector>> &v_cat_lin_1_struct = duckdb::StructVector::GetEntries(
                    v_cat_lin_1_values);
            //todo flatvector assumption
            cat_set_val_key = duckdb::FlatVector::GetData<int>(*((v_cat_lin_1_struct)[0]));
            cat_set_val_val = duckdb::FlatVector::GetData<float>(*((v_cat_lin_1_struct)[1]));

        }

        for (idx_t j = 0; j < count; j++) {
            auto state = states[sdata.sel->get_index(j)];
            for(idx_t k=0; k<cat_attr; k++){
                auto curr_metadata = sublist_metadata[k + (j*cat_attr)];
                auto &col_vals_state = state->lin_cat[k];
                for (size_t item = 0; item < curr_metadata.length; item++){//for each column values
                    int key = cat_set_val_key[item+curr_metadata.offset]; // v_struct[0].GetValue<int>();
                    auto pos = col_vals_state.find(key);
                    if (pos == col_vals_state.end())
                        col_vals_state[key] = cat_set_val_val[item+curr_metadata.offset];
                    else
                        pos->second += cat_set_val_val[item+curr_metadata.offset];
                }
            }
        }

        //set num*cat
        Vector &c5 = ListVector::GetEntry(*children[4]);

        if(cat_attr > 0){
            Vector v_cat_lin_1 = duckdb::ListVector::GetEntry(*children[4]);
            sublist_metadata = duckdb::ListVector::GetData(v_cat_lin_1);
            Vector v_cat_lin_1_values = duckdb::ListVector::GetEntry(v_cat_lin_1);
            vector<unique_ptr<duckdb::Vector>> &v_cat_lin_1_struct = duckdb::StructVector::GetEntries(
                    v_cat_lin_1_values);
            //todo flatvector assumption
            cat_set_val_key = duckdb::FlatVector::GetData<int>(*((v_cat_lin_1_struct)[0]));
            cat_set_val_val = duckdb::FlatVector::GetData<float>(*((v_cat_lin_1_struct)[1]));
        }

        for (idx_t j = 0; j < count; j++) {
            auto state = states[sdata.sel->get_index(j)];
            for(idx_t k=0; k<cat_attr * num_attr; k++){
                auto curr_metadata = sublist_metadata[k + (j*(cat_attr * num_attr))];
                auto &col_vals = state->quad_num_cat[k];
                for (size_t item = 0; item < curr_metadata.length; item++){
                    int key = cat_set_val_key[item + curr_metadata.offset];
                    auto pos = col_vals.find(key);
                    if (pos == col_vals.end())
                        col_vals[key] = cat_set_val_val[item+curr_metadata.offset];
                    else
                        pos->second += cat_set_val_val[item+curr_metadata.offset];
                }
            }
        }

        Vector &c6 = ListVector::GetEntry(*children[5]);
        int *cat_set_val_key_1 = nullptr;
        int *cat_set_val_key_2 = nullptr;
        if(cat_attr > 0){
            Vector v_cat_lin_1 = duckdb::ListVector::GetEntry(*children[5]);
            sublist_metadata = duckdb::ListVector::GetData(v_cat_lin_1);
            Vector v_cat_lin_1_values = duckdb::ListVector::GetEntry(v_cat_lin_1);
            vector<unique_ptr<duckdb::Vector>> &v_cat_lin_1_struct = duckdb::StructVector::GetEntries(
                    v_cat_lin_1_values);
            //todo flatvector assumption
            cat_set_val_key_1 = duckdb::FlatVector::GetData<int>(*((v_cat_lin_1_struct)[0]));
            cat_set_val_key_2 = duckdb::FlatVector::GetData<int>(*((v_cat_lin_1_struct)[1]));
            cat_set_val_val = duckdb::FlatVector::GetData<float>(*((v_cat_lin_1_struct)[2]));
        }

        for (idx_t j = 0; j < count; j++) {
            auto state = states[sdata.sel->get_index(j)];
            for(idx_t k=0; k<(cat_attr * (cat_attr+1))/2; k++){
                auto curr_metadata = sublist_metadata[k + (j*(cat_attr * (cat_attr+1))/2)];
                auto &col_vals = state->quad_cat_cat[k];
                for (size_t item = 0; item < curr_metadata.length; item++){
                    std::pair<int, int> key = std::pair<int, int>(cat_set_val_key_1[item + curr_metadata.offset], cat_set_val_key_2[item + curr_metadata.offset]);
                    auto pos = col_vals.find(key);
                    if (pos == col_vals.end())
                        col_vals[key] = cat_set_val_val[item + curr_metadata.offset];
                    else
                        pos->second += cat_set_val_val[item + curr_metadata.offset];
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
            if (combined_ptr[i]->lin_agg == nullptr && combined_ptr[i]->lin_cat == nullptr) {//init

                combined_ptr[i]->num_attributes = state->num_attributes;
                combined_ptr[i]->cat_attributes = state->cat_attributes;

                if(state->num_attributes > 0) {
                    combined_ptr[i]->lin_agg = new float[state->num_attributes + ((state->num_attributes * (state->num_attributes + 1)) /
                                                                                  2)];
                    combined_ptr[i]->quadratic_agg = &(combined_ptr[i]->lin_agg[state->num_attributes]);

                    for (idx_t k = 0; k < (state->num_attributes * (state->num_attributes + 1)) / 2; k++)
                        combined_ptr[i]->quadratic_agg[k] = 0;
                    for (idx_t k = 0; k < state->num_attributes; k++)
                        combined_ptr[i]->lin_agg[k] = 0;
                }

                if(state->cat_attributes > 0) {
                    combined_ptr[i]->lin_cat = new boost::container::flat_map<int, float>[state->cat_attributes + (state->num_attributes *
                                                                                                           state->cat_attributes)];
                    if(state->num_attributes > 0)
                        combined_ptr[i]->quad_num_cat = &(combined_ptr[i]->lin_cat[state->cat_attributes]);

                    combined_ptr[i]->quad_cat_cat = new boost::container::flat_map<std::pair<int, int>, float>[
                    state->cat_attributes * (state->cat_attributes + 1) / 2];
                }
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


        //std::cout<<"Start fin"<<std::endl;
        assert(offset == 0);
                D_ASSERT(result.GetType().id() == duckdb::LogicalTypeId::STRUCT);

        duckdb::UnifiedVectorFormat sdata;
        state_vector.ToUnifiedFormat(count, sdata);
        auto states = (Triple::AggState **) sdata.data;
        auto &children = duckdb::StructVector::GetEntries(result);
        Vector &c1 = *(children[0]);//N
        auto input_data = (int32_t *) FlatVector::GetData(c1);

        for (idx_t i=0;i<count; i++) {
            const auto row_id = i + offset;
            input_data[row_id] = states[sdata.sel->get_index(i)]->count;
        }

        //Set List
        Vector &c2 = *(children[1]);
                D_ASSERT(c2.GetType().id() == LogicalTypeId::LIST);
        Vector &c3 = *(children[2]);
                D_ASSERT(c3.GetType().id() == LogicalTypeId::LIST);
        if(count > 0 && states[sdata.sel->get_index(0)]->num_attributes > 0) {
            duckdb::ListVector::Reserve(c2, states[sdata.sel->get_index(0)]->num_attributes * count);
            duckdb::ListVector::SetListSize(c2, states[sdata.sel->get_index(0)]->num_attributes * count);

            duckdb::ListVector::Reserve(c3, ((states[sdata.sel->get_index(0)]->num_attributes * (states[sdata.sel->get_index(0)]->num_attributes +1))/2) * count);
            duckdb::ListVector::SetListSize(c3, ((states[sdata.sel->get_index(0)]->num_attributes * (states[sdata.sel->get_index(0)]->num_attributes +1))/2) * count);
        }

        c2.SetVectorType(VectorType::FLAT_VECTOR);
        auto result_data = ListVector::GetData(c2);
        auto num_lin_res = FlatVector::GetData<float>(ListVector::GetEntry(c2));
        auto num_cat_res = FlatVector::GetData<float>(ListVector::GetEntry(c3));

        for (idx_t i = 0; i < count; i++) {
            auto state = states[sdata.sel->get_index(i)];
            const auto row_id = i + offset;
            for (int j = 0; j < state->num_attributes; j++) {
                num_lin_res[j + (i*state->num_attributes)] = state->lin_agg[j];
            }
            result_data[row_id].length = state->num_attributes;
            result_data[row_id].offset = result_data[i].length*i;//ListVector::GetListSize(c2);
            //std::cout<<"linear len: "<<result_data[row_id].length<<"linear offs: "<<result_data[row_id].offset<<std::endl;
        }

        //set quadratic attributes
        c3.SetVectorType(VectorType::FLAT_VECTOR);
        auto result_data2 = ListVector::GetData(c3);
        for (idx_t i = 0; i < count; i++) {
            auto state = states[sdata.sel->get_index(i)];
            const auto row_id = i + offset;
            for (int j = 0; j < state->num_attributes*(state->num_attributes+1)/2; j++) {
                num_cat_res[j + (i*(state->num_attributes*(state->num_attributes+1)/2))] = state->quadratic_agg[j];
            }//Value::Numeric
            result_data2[row_id].length = state->num_attributes*(state->num_attributes+1)/2;
            result_data2[row_id].offset = i * result_data2[i].length;
            //std::cout<<"quad len: "<<result_data2[row_id].length<<"quad offs: "<<result_data2[row_id].offset<<std::endl;
        }

        //categorical sums

        Vector &c4 = *(children[3]);
                D_ASSERT(c4.GetType().id() == LogicalTypeId::LIST);
        c4.SetVectorType(VectorType::FLAT_VECTOR);
        auto result_data3 = ListVector::GetData(c4);

        //num*cat
        Vector &c5 = *(children[4]);
                D_ASSERT(c5.GetType().id() == LogicalTypeId::LIST);
        c5.SetVectorType(VectorType::FLAT_VECTOR);
        auto result_data4 = ListVector::GetData(c5);

        //cat*cat
        Vector &c6 = *(children[5]);
                D_ASSERT(c6.GetType().id() == LogicalTypeId::LIST);
        c6.SetVectorType(VectorType::FLAT_VECTOR);
        auto result_data5 = ListVector::GetData(c6);

        list_entry_t *sublist_metadata = nullptr;
        int *cat_set_val_key = nullptr;
        float *cat_set_val_val = nullptr;

        int cat_attributes = 0;
        int num_attributes = 0;

        if (count > 0 && states[sdata.sel->get_index(0)]->cat_attributes > 0) {
            //init list size
            cat_attributes = states[sdata.sel->get_index(0)]->cat_attributes;
            num_attributes = states[sdata.sel->get_index(0)]->num_attributes;

            int n_items = 0;
            int n_items_cat_cat = 0;
            for (idx_t i = 0; i < count; i++) {
                auto state = states[sdata.sel->get_index(i)];
                for(idx_t j=0; j<cat_attributes; j++)
                    n_items += state->lin_cat[j].size();
                for(idx_t j=0; j<(cat_attributes*(cat_attributes+1))/2; j++)
                    n_items_cat_cat += state->quad_cat_cat[j].size();
            }

            {
                c4.SetVectorType(VectorType::FLAT_VECTOR);
                Vector cat_relations_vector = duckdb::ListVector::GetEntry(c4);
                duckdb::ListVector::Reserve(cat_relations_vector, n_items);
                duckdb::ListVector::SetListSize(cat_relations_vector, n_items);
                duckdb::ListVector::Reserve(c4, cat_attributes * count);
                duckdb::ListVector::SetListSize(c4, cat_attributes * count);
            }
            {
                c5.SetVectorType(VectorType::FLAT_VECTOR);
                Vector cat_relations_vector = duckdb::ListVector::GetEntry(c5);
                duckdb::ListVector::Reserve(cat_relations_vector, n_items * num_attributes);
                duckdb::ListVector::SetListSize(cat_relations_vector, n_items * num_attributes);
                duckdb::ListVector::Reserve(c5, cat_attributes * num_attributes * count);
                duckdb::ListVector::SetListSize(c5, cat_attributes * num_attributes * count);
            }
            {
                c6.SetVectorType(VectorType::FLAT_VECTOR);
                Vector cat_relations_vector = duckdb::ListVector::GetEntry(c6);
                duckdb::ListVector::Reserve(cat_relations_vector, n_items_cat_cat);
                duckdb::ListVector::SetListSize(cat_relations_vector, n_items_cat_cat);
                duckdb::ListVector::Reserve(c6, ((cat_attributes * (cat_attributes+1))/2) * count);
                duckdb::ListVector::SetListSize(c6, ((cat_attributes * (cat_attributes+1))/2) * count);
            }
        }

        idx_t sublist_idx = 0;
        idx_t skipped = 0;
        idx_t idx_element = 0;
        if(cat_attributes > 0){
            Vector cat_relations_vector = duckdb::ListVector::GetEntry(c4);
            sublist_metadata = duckdb::ListVector::GetData(cat_relations_vector);
            Vector cat_relations_vector_sub = duckdb::ListVector::GetEntry(
                    cat_relations_vector);//this is a sequence of values (struct in our case, 2 vectors)
            vector<unique_ptr<duckdb::Vector>> &lin_struct_vector = duckdb::StructVector::GetEntries(
                    cat_relations_vector_sub);
            c4.SetVectorType(VectorType::FLAT_VECTOR);
            ((lin_struct_vector)[0])->SetVectorType(VectorType::FLAT_VECTOR);
            ((lin_struct_vector)[1])->SetVectorType(VectorType::FLAT_VECTOR);
            cat_set_val_key = duckdb::FlatVector::GetData<int>(*((lin_struct_vector)[0]));
            cat_set_val_val = duckdb::FlatVector::GetData<float>(*((lin_struct_vector)[1]));

        }
        //problem is here

        for (idx_t i = 0; i < count; i++) {
            auto state = states[sdata.sel->get_index(i)];
            const auto row_id = i + offset;

            for (int j = 0; j < state->cat_attributes; j++) {
                std::map<int, float> ordered(state->lin_cat[j].begin(), state->lin_cat[j].end());
                for (auto const& state_val : ordered){
                    cat_set_val_key[idx_element] = state_val.first;
                    cat_set_val_val[idx_element] = state_val.second;
                    idx_element++;
                }
                sublist_metadata[sublist_idx].length = ordered.size();
                sublist_metadata[sublist_idx].offset = skipped;
                skipped += ordered.size();
                sublist_idx++;
            }
            result_data3[row_id].length = state->cat_attributes;
            result_data3[row_id].offset = i * result_data3[row_id].length;
            //std::cout<<"cat len: "<<result_data3[row_id].length<<"cat offs: "<<result_data3[row_id].offset<<std::endl;
        }

        //categorical num*cat

        if(cat_attributes > 0) {
            Vector cat_relations_vector = duckdb::ListVector::GetEntry(c5);
            sublist_metadata = duckdb::ListVector::GetData(cat_relations_vector);
            Vector cat_relations_vector_sub = duckdb::ListVector::GetEntry(
                    cat_relations_vector);//this is a sequence of values (struct in our case, 2 vectors)
            vector<unique_ptr<duckdb::Vector>> &lin_struct_vector = duckdb::StructVector::GetEntries(
                    cat_relations_vector_sub);
            ((lin_struct_vector)[0])->SetVectorType(VectorType::FLAT_VECTOR);
            ((lin_struct_vector)[1])->SetVectorType(VectorType::FLAT_VECTOR);
            cat_set_val_key = duckdb::FlatVector::GetData<int>(*((lin_struct_vector)[0]));
            cat_set_val_val = duckdb::FlatVector::GetData<float>(*((lin_struct_vector)[1]));
        }

        idx_element = 0;
        sublist_idx = 0;
        skipped = 0;
        for (idx_t i = 0; i < count; i++) {
            auto state = states[sdata.sel->get_index(i)];
            const auto row_id = i + offset;

            for (int j = 0; j < state->cat_attributes * state->num_attributes; j++) {
                //std::cout<<state->quad_num_cat[j].begin()->first<<" "<<state->quad_num_cat[j].begin()->second;
                std::map<int, float> ordered(state->quad_num_cat[j].begin(), state->quad_num_cat[j].end());
                for (auto const& state_val : ordered){
                    cat_set_val_key[idx_element] = state_val.first;
                    cat_set_val_val[idx_element] = state_val.second;
                    idx_element++;
                }
                sublist_metadata[sublist_idx].length = ordered.size();
                sublist_metadata[sublist_idx].offset = skipped;
                skipped += ordered.size();
                sublist_idx++;
            }
            result_data4[row_id].length = state->cat_attributes * state->num_attributes;
            result_data4[row_id].offset = i * result_data4[row_id].length;
            //std::cout<<"numcat len: "<<result_data4[row_id].length<<"numcat offs: "<<result_data4[row_id].offset<<std::endl;
        }

        //categorical cat*cat

        int *cat_set_val_key_1 = nullptr;
        int *cat_set_val_key_2 = nullptr;

        if(cat_attributes > 0) {
            Vector cat_relations_vector = duckdb::ListVector::GetEntry(c6);
            sublist_metadata = duckdb::ListVector::GetData(cat_relations_vector);
            Vector cat_relations_vector_sub = duckdb::ListVector::GetEntry(
                    cat_relations_vector);//this is a sequence of values (struct in our case, 2 vectors)
            vector<unique_ptr<duckdb::Vector>> &lin_struct_vector = duckdb::StructVector::GetEntries(
                    cat_relations_vector_sub);
            ((lin_struct_vector)[0])->SetVectorType(VectorType::FLAT_VECTOR);
            ((lin_struct_vector)[1])->SetVectorType(VectorType::FLAT_VECTOR);
            ((lin_struct_vector)[2])->SetVectorType(VectorType::FLAT_VECTOR);
            cat_set_val_key_1 = duckdb::FlatVector::GetData<int>(*((lin_struct_vector)[0]));
            cat_set_val_key_2 = duckdb::FlatVector::GetData<int>(*((lin_struct_vector)[1]));
            cat_set_val_val = duckdb::FlatVector::GetData<float>(*((lin_struct_vector)[2]));
        }

        idx_element = 0;
        sublist_idx = 0;
        skipped = 0;

        for (idx_t i = 0; i < count; i++) {
            auto state = states[sdata.sel->get_index(i)];
            const auto row_id = i + offset;

            for (int j = 0; j < state->cat_attributes * (state->cat_attributes+1)/2; j++) {
                std::map<std::pair<int, int>, float> ordered(state->quad_cat_cat[j].begin(), state->quad_cat_cat[j].end());
                for (auto const& state_val : ordered){
                    cat_set_val_key_1[idx_element] = state_val.first.first;
                    cat_set_val_key_2[idx_element] = state_val.first.second;
                    cat_set_val_val[idx_element] = state_val.second;
                    idx_element++;
                }
                sublist_metadata[sublist_idx].length = ordered.size();
                sublist_metadata[sublist_idx].offset = skipped;
                skipped += ordered.size();
                sublist_idx++;
            }
            result_data5[row_id].length = state->cat_attributes * (state->cat_attributes+1) / 2;
            result_data5[row_id].offset = i * result_data5[row_id].length;
            //std::cout<<"catcat len: "<<result_data5[row_id].length<<"catcat offs: "<<result_data5[row_id].offset<<std::endl;
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
        for(idx_t i=0;i<cat_linear_1.size();i++) {//for each cat. column copy into map
            const vector<Value> &pairs_cat_col_1 = duckdb::ListValue::GetChildren(cat_linear_1[i]);
            const vector<Value> &pairs_cat_col_2 = duckdb::ListValue::GetChildren(cat_linear_2[i]);
            cat_lin.push_back(duckdb::Value::LIST(sum_list_of_structs(pairs_cat_col_1, pairs_cat_col_2)));
        }
        struct_values.emplace_back("lin_cat", duckdb::Value::LIST(cat_lin));

        //num*cat

        const vector<Value> &num_cat_1 = duckdb::ListValue::GetChildren(first_triple_children[4]);
        const vector<Value> &num_cat_2 = duckdb::ListValue::GetChildren(sec_triple_children[4]);
        vector<Value> num_cat_res = {};
        for(idx_t i=0;i<num_cat_1.size();i++) {//for each cat. column copy into map
            const vector<Value> &pairs_cat_col_1 = duckdb::ListValue::GetChildren(num_cat_1[i]);
            const vector<Value> &pairs_cat_col_2 = duckdb::ListValue::GetChildren(num_cat_2[i]);
            num_cat_res.push_back(duckdb::Value::LIST(sum_list_of_structs(pairs_cat_col_1, pairs_cat_col_2)));
        }
        struct_values.emplace_back("quad_num_cat", duckdb::Value::LIST(num_cat_res));

        //cat*cat

        const vector<Value> &cat_cat_1 = duckdb::ListValue::GetChildren(first_triple_children[5]);
        const vector<Value> &cat_cat_2 = duckdb::ListValue::GetChildren(sec_triple_children[5]);
        vector<Value> cat_cat_res = {};
        for(idx_t i=0;i<cat_cat_1.size();i++) {//for each cat. column copy into map
            const vector<Value> &pairs_cat_col_1 = duckdb::ListValue::GetChildren(cat_cat_1[i]);
            const vector<Value> &pairs_cat_col_2 = duckdb::ListValue::GetChildren(cat_cat_2[i]);
            cat_cat_res.push_back(duckdb::Value::LIST(sum_list_of_structs_key2(pairs_cat_col_1, pairs_cat_col_2)));
        }
        struct_values.emplace_back("quad_cat", duckdb::Value::LIST(cat_cat_res));

        auto ret = duckdb::Value::STRUCT(struct_values);
        return ret;
    }

}