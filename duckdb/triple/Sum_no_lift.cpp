//
// Created by Massimo Perini on 03/06/2023.
//

#include "Sum_no_lift.h"

#include "Triple_sum.h"
#include "From_duckdb.h"

#include <iostream>

duckdb::unique_ptr<duckdb::FunctionData>
Triple::SumNoLiftBind(duckdb::ClientContext &context, duckdb::AggregateFunction &function,
                 duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &arguments) {

    //std::cout<<arguments.size()<<"\n";
    //std::cout<<(function.arguments.size())<<"\n";

    child_list_t<LogicalType> struct_children;
    struct_children.emplace_back("N", LogicalType::INTEGER);
    struct_children.emplace_back("lin_agg", LogicalType::LIST(LogicalType::FLOAT));
    struct_children.emplace_back("quad_agg", LogicalType::LIST(LogicalType::FLOAT));

    auto struct_type = LogicalType::STRUCT(struct_children);
    function.return_type = struct_type;
    //set arguments
    function.varargs = LogicalType::FLOAT;
    return duckdb::make_uniq<duckdb::VariableReturnBindData>(function.return_type);
}


//Updates the state with a new value. E.g, For SUM, this adds the value to the state
void Triple::SumNoLift(duckdb::Vector inputs[], duckdb::AggregateInputData &aggr_input_data, idx_t input_count,
                        duckdb::Vector &state_vector, idx_t count) {
    //count: count of tuples to process
    //inputs: inputs to process

    duckdb::UnifiedVectorFormat sdata;
    state_vector.ToUnifiedFormat(count, sdata);

    auto states = (Triple::AggState **) sdata.data;

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
        auto state = states[sdata.sel->get_index(j)];

        if (state->attributes == 0){//initialize
            state->attributes = input_count;
            state->lin_agg = new float[input_count];
            for(idx_t k=0; k<input_count; k++)
                state->lin_agg[k] = 0;
            state->quadratic_agg = new float[(input_count*(input_count+1))/2];
            for(idx_t k=0; k<(input_count*(input_count+1))/2; k++)
                state->quadratic_agg[k] = 0;
        }

        for(idx_t k=0; k<input_count; k++) {
            state->lin_agg[k] += ((float *) inputs[k].GetData())[j];
        }
    }

    //SUM QUADRATIC AGGREGATES:
    int col_idx = 0;

    for(idx_t j=0; j<input_count; j++) {
        float *first_column_data = (float *) inputs[j].GetData();
        for (idx_t k=j;k<input_count; k++){
            float *sec_column_data = (float *) inputs[k].GetData();
            for (idx_t i = 0; i < count; i++) {
                auto state = states[sdata.sel->get_index(i)];
                state->quadratic_agg[col_idx] += first_column_data[i] * sec_column_data[i];
            }
            col_idx++;
        }
    }
}

void
Triple::SumNoLiftCombine(duckdb::Vector &state, duckdb::Vector &combined, duckdb::AggregateInputData &aggr_input_data,
                    idx_t count) {
    //std::cout<<"\nCOMBINE START"<<std::endl;

    duckdb::UnifiedVectorFormat sdata;
    state.ToUnifiedFormat(count, sdata);
    auto states_ptr = (Triple::AggState **) sdata.data;

    auto combined_ptr = duckdb::FlatVector::GetData<Triple::AggState *>(combined);

    for (idx_t i = 0; i < count; i++) {
        auto state = states_ptr[sdata.sel->get_index(i)];
        combined_ptr[i]->count += state->count;
        if (combined_ptr[i]->attributes == 0) {

            combined_ptr[i]->attributes = state->attributes;
            combined_ptr[i]->lin_agg = new float[state->attributes];
            for(idx_t k=0; k<state->attributes; k++)
                combined_ptr[i]->lin_agg[k] = 0;
            combined_ptr[i]->quadratic_agg = new float[(state->attributes*(state->attributes+1))/2];
            for(idx_t k=0; k<(state->attributes*(state->attributes+1))/2; k++)
                combined_ptr[i]->quadratic_agg[k] = 0;

        }

        for (int j = 0; j < state->attributes; j++) {
            combined_ptr[i]->lin_agg[j] += state->lin_agg[j];
        }
        for (int j = 0; j < state->attributes*(state->attributes+1)/2; j++) {
            combined_ptr[i]->quadratic_agg[j] += state->quadratic_agg[j];
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
    Vector c1 = *(children[0]);//N
    auto input_data = (int32_t *) FlatVector::GetData(c1);

    for (idx_t i=0;i<count; i++) {
        const auto row_id = i + offset;
        input_data[row_id] = states[sdata.sel->get_index(i)]->count;
    }
    //Set List
    Vector c2 = *(children[1]);
            D_ASSERT(c2.GetType().id() == LogicalTypeId::LIST);
    c2.SetVectorType(VectorType::FLAT_VECTOR);
    auto result_data = FlatVector::GetData<list_entry_t>(c2);
    for (idx_t i = 0; i < count; i++) {
        auto state = states[sdata.sel->get_index(i)];
        const auto row_id = i + offset;
        for (int j = 0; j < state->attributes; j++) {
            ListVector::PushBack(c2,
                                 Value(state->lin_agg[j + (i*state->attributes)]));//Value::STRUCT({std::make_pair("key", bucket_value), std::make_pair("value", count_value)});
            //ListVector::Reserve(c2, offset);
            //ListVector::SetListSize(result, offset);
            //*c2.auxiliary->SetValue
            //	child->SetValue(size++, insert);
        }//Value::Numeric
        //Set metadata for current row
        result_data[row_id].length = state->attributes;
        result_data[row_id].offset = result_data[i].length*i;//ListVector::GetListSize(c2);
    }

    Vector c3 = *(children[2]);
            D_ASSERT(c3.GetType().id() == LogicalTypeId::LIST);
    c3.SetVectorType(VectorType::FLAT_VECTOR);
    auto result_data2 = FlatVector::GetData<list_entry_t>(c3);
    for (idx_t i = 0; i < count; i++) {
        auto state = states[sdata.sel->get_index(i)];
        const auto row_id = i + offset;
        for (int j = 0; j < state->attributes*(state->attributes+1)/2; j++) {
            ListVector::PushBack(c3,
                                 Value(state->quadratic_agg[j]));//Value::STRUCT({std::make_pair("key", bucket_value), std::make_pair("value", count_value)});
        }//Value::Numeric
        result_data2[row_id].length = state->attributes*(state->attributes+1)/2;
        result_data2[row_id].offset = i * result_data2[i].length;
    }
    //std::cout<<"\nFINALIZE END\n";
}