//
// Created by Massimo Perini on 03/06/2023.
//

#include "Sum_no_lift.h"
#include <duckdb/function/scalar/nested_functions.hpp>

#include "Triple_sum.h"
#include "From_duckdb.h"

#include <iostream>
#include <map>
#include <unordered_map>
#include <boost/functional/hash.hpp>
//#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_map.hpp>


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
    //std::cout<<"Start SUMNOLIFT";

    for (idx_t j = 0; j < count; j++) {
        size_t curr_numerical = 0;
        size_t curr_cateogrical = 0;
        auto state = states[sdata.sel->get_index(j)];
        //initialize
        if (state->lin_agg == nullptr && state->lin_agg == nullptr){
            state->num_attributes = num_cols;
            state->cat_attributes = cat_cols;

            if(num_cols > 0) {
                state->lin_agg = new float[num_cols + ((num_cols * (num_cols + 1)) / 2)];
                state->quadratic_agg = &(state->lin_agg[num_cols]);
                for (idx_t k = 0; k < num_cols; k++)
                    state->lin_agg[k] = 0;
                for (idx_t k = 0; k < (num_cols * (num_cols + 1)) / 2; k++)
                    state->quadratic_agg[k] = 0;
            }

            if(cat_cols > 0) {
                state->lin_cat = new std::unordered_map<int, float>[cat_cols + (num_cols * cat_cols)];
                if (num_cols > 0)
                    state->quad_num_cat = &(state->lin_cat[cat_cols]);
                state->quad_cat_cat = new std::unordered_map<std::pair<int, int>, float, boost::hash<pair<int, int>>>[
                cat_cols * (cat_cols + 1) / 2];
            }
        }


        for(idx_t k=0; k<cols; k++) {
            auto col_type = inputs[k].GetType();
            if (col_type == LogicalType::FLOAT || col_type == LogicalType::DOUBLE) {//sum numerical
                state->lin_agg[curr_numerical] += UnifiedVectorFormat::GetData<float>(input_data[k])[input_data[k].sel->get_index(j)];
                curr_numerical++;
            }
            else{//sum categorical
                int in_col_val = UnifiedVectorFormat::GetData<int>(input_data[k])[input_data[k].sel->get_index(j)];
                std::unordered_map<int, float> &col_vals = state->lin_cat[curr_cateogrical];
                auto pos = col_vals.find(in_col_val);
                if (pos == col_vals.end())
                    col_vals[in_col_val] = 1;
                else
                    pos->second++;
                curr_cateogrical++;
            }
        }
    }

    //std::cout<<"End INIT + lin NEW"<<std::endl;


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
    //std::cout<<"Start NUMCAT"<<std::endl;


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
                        std::unordered_map<int, float> &vals = state->quad_num_cat[col_idx];
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
    //std::cout<<"End NUMCAT"<<std::endl;

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
                        auto &vals = state->quad_cat_cat[col_idx];
                        auto entry = std::pair<int, int>(first_column_data[input_data[j].sel->get_index(i)], sec_column_data[input_data[k].sel->get_index(i)]);
                        auto pos = vals.find(entry);
                        if (pos == vals.end()) {
                            vals[entry] = 1;
                        }
                        else {
                            pos->second += 1;
                        }
                    }
                    col_idx++;
                }
            }
        }
    }
    //std::cout<<"END SUMNOLIFT"<<std::endl;
}

void
Triple::SumNoLiftCombine(duckdb::Vector &state, duckdb::Vector &combined, duckdb::AggregateInputData &aggr_input_data,
                    idx_t count) {
    //std::cout<<"\nstart combine\n"<<std::endl;

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
                combined_ptr[i]->lin_agg = new float[state->num_attributes +
                                                     ((state->num_attributes * (state->num_attributes + 1)) / 2)];

                combined_ptr[i]->quadratic_agg = &(combined_ptr[i]->lin_agg[state->num_attributes]);

                for (idx_t k = 0; k < state->num_attributes; k++)
                    combined_ptr[i]->lin_agg[k] = 0;

                for (idx_t k = 0; k < (state->num_attributes * (state->num_attributes + 1)) / 2; k++)
                    combined_ptr[i]->quadratic_agg[k] = 0;
            }

            if(state->cat_attributes > 0) {
                combined_ptr[i]->lin_cat = new std::unordered_map<int, float>[state->cat_attributes +
                                                                              (state->num_attributes *
                                                                               state->cat_attributes)];
                if(state->num_attributes > 0)
                    combined_ptr[i]->quad_num_cat = &(combined_ptr[i]->lin_cat[state->cat_attributes]);

                combined_ptr[i]->quad_cat_cat = new std::unordered_map<std::pair<int, int>, float, boost::hash<std::pair<int, int>>>[
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
    //std::cout<<"\nCOMBINE END\n"<<std::endl;
}

void Triple::SumNoLiftFinalize(duckdb::Vector &state_vector, duckdb::AggregateInputData &aggr_input_data, duckdb::Vector &result,
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
    //std::cout<<"end fin"<<std::endl;

}