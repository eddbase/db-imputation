//
// Created by Massimo Perini on 30/05/2023.
//

#include "Triple_sum.h"
#include "From_duckdb.h"

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


        if (arguments[0]->return_type.id() == duckdb::LogicalTypeId::UNKNOWN) {
            function.arguments[0] = duckdb::LogicalTypeId::UNKNOWN;
            function.return_type = duckdb::LogicalType::SQLNULL;
            return nullptr;
        }
        function.return_type = arguments[0]->return_type;
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
        Vector c1 = *children[0];//this should be the vector of integers....
        auto input_data = (int32_t *) FlatVector::GetData(c1);
        //auto state = states[sdata.sel->get_index(0)];//because of parallelism???

        for (int j = 0; j < count; j++) {
            auto state = states[sdata.sel->get_index(j)];
            state->count += input_data[j];
        }
        //auto N =  input_data;

        //SUM LINEAR AGGREGATES:
        Vector c2 = *children[1];//this should be the linear aggregate....
        auto lists_size = ListVector::GetListSize(c2);
        auto list_entries = (float *) ListVector::GetEntry(c2).GetData();//entries are float
        int cols = lists_size / count;//cols = total values / num. rows

        for (idx_t j = 0; j < count; j++) {
            auto state = states[sdata.sel->get_index(j)];

            if (state->attributes == 0){//initialize
                state->attributes = cols;
                state->lin_agg = new float[cols];
                for(idx_t k=0; k<cols; k++)
                    state->lin_agg[k] = 0;
                state->quadratic_agg = new float[(cols*(cols+1))/2];
                for(idx_t k=0; k<(cols*(cols+1))/2; k++)
                    state->quadratic_agg[k] = 0;
                //state->lin_agg.resize(cols);
                //std::fill(state->lin_agg.begin(), state->lin_agg.end(), 0);
            }
            for(idx_t k=0; k<cols; k++) {
                state->lin_agg[k] += list_entries[k + (j * cols)];
                //std::cout<<"SUM: "<<list_entries[k + (j * cols)]<<" STATE VALUE: "<<state->lin_agg[k]<<std::endl;
            }
        }

        //SUM QUADRATIC AGGREGATES:
        Vector c3 = *children[2];//this should be the linear aggregate....
        auto lists_size2 = ListVector::GetListSize(c3);
        auto list_entries_2 = (float *) ListVector::GetEntry(c3).GetData();//entries are float
        int cols2 = ((cols * (cols + 1)) / 2);//quadratic aggregates columns

        for (idx_t j = 0; j < count; j++) {
            auto state = states[sdata.sel->get_index(j)];
            //std::cout<<"state index: "<<sdata.sel->get_index(j)<<std::endl;
            /*if (state->quadratic_agg.empty()){
                state->quadratic_agg.resize(cols2);
                std::fill(state->quadratic_agg.begin(), state->quadratic_agg.end(), 0);
            }*/
            for(idx_t k=0; k<cols2; k++)
                state->quadratic_agg[k] += list_entries_2[k + (j*cols2)];
        }
        //for (int i=0;i<states[0]->quadratic_agg.size();i++)
        //    std::cout << "STATS " << states[0]->quadratic_agg[i]<<"\n";
        //std::cout<<"\nUPDATE END"<<std::endl;
    }

    void
    ListCombineFunction(duckdb::Vector &state, duckdb::Vector &combined, duckdb::AggregateInputData &aggr_input_data,
                        idx_t count) {
        //std::cout<<"\nCOMBINE START"<<std::endl;

        duckdb::UnifiedVectorFormat sdata;
        state.ToUnifiedFormat(count, sdata);
        auto states_ptr = (AggState **) sdata.data;

        auto combined_ptr = duckdb::FlatVector::GetData<AggState *>(combined);

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

                //combined_ptr[i]->lin_agg.resize(state->lin_agg.size());
                //std::fill(combined_ptr[i]->lin_agg.begin(), combined_ptr[i]->lin_agg.end(), 0);
            }
            /*if (combined_ptr[i]->quadratic_agg.empty()) {
                combined_ptr[i]->quadratic_agg.resize(state->quadratic_agg.size());
                std::fill(combined_ptr[i]->quadratic_agg.begin(), combined_ptr[i]->quadratic_agg.end(), 0);
            }*/

            for (int j = 0; j < state->attributes; j++) {
                combined_ptr[i]->lin_agg[j] += state->lin_agg[j];
            }
            for (int j = 0; j < state->attributes*(state->attributes+1)/2; j++) {
                combined_ptr[i]->quadratic_agg[j] += state->quadratic_agg[j];
            }
        }
        //std::cout<<"\nCOMBINE END\n";
    }

    void ListFinalize(duckdb::Vector &state_vector, duckdb::AggregateInputData &aggr_input_data, duckdb::Vector &result,
                      idx_t count,
                      idx_t offset) {
        //std::cout<<"FINALIZE START count: "<<count<<"offset "<<offset<<std::endl;
        duckdb::UnifiedVectorFormat sdata;
        state_vector.ToUnifiedFormat(count, sdata);
        auto states = (AggState **) sdata.data;
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
}