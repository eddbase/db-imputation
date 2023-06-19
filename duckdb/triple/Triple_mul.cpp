//
// Created by Massimo Perini on 30/05/2023.
//

#include "Triple_mul.h"
#include <duckdb/function/scalar/nested_functions.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/storage/statistics/list_stats.hpp>

#include <map>
#include <iostream>
#include <algorithm>
#include "From_duckdb.h"

//DataChunk is a set of vectors

namespace Triple {
    //actual implementation of this function
    void StructPackFunction(duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result) {
        //std::cout << "StructPackFunction start " << std::endl;
        auto &result_children = duckdb::StructVector::GetEntries(result);

        idx_t size = args.size();//n. of rows to return

        auto &first_triple_children = duckdb::StructVector::GetEntries(args.data[0]);//vector of pointers to childrens
        auto &sec_triple_children = duckdb::StructVector::GetEntries(args.data[1]);

        //set N

        RecursiveFlatten(*first_triple_children[0], size);
                D_ASSERT((*first_triple_children[0]).GetVectorType() == duckdb::VectorType::FLAT_VECTOR);
        RecursiveFlatten(*sec_triple_children[0], size);
                D_ASSERT((*sec_triple_children[0]).GetVectorType() == duckdb::VectorType::FLAT_VECTOR);


        auto N_1 = (int32_t *) duckdb::FlatVector::GetData(*first_triple_children[0]);
        auto N_2 = (int32_t *) duckdb::FlatVector::GetData(*sec_triple_children[0]);
        auto input_data = (int32_t *) duckdb::FlatVector::GetData(*result_children[0]);

        for (idx_t i = 0; i < size; i++) {
            input_data[i] = N_1[i] * N_2[i];
        }

        //set linear aggregates
        std::cout<<"set linear aggregates\n";
        RecursiveFlatten(*first_triple_children[1], size);
                D_ASSERT((*first_triple_children[1]).GetVectorType() == duckdb::VectorType::FLAT_VECTOR);
        RecursiveFlatten(*sec_triple_children[1], size);
                D_ASSERT((*sec_triple_children[1]).GetVectorType() == duckdb::VectorType::FLAT_VECTOR);

        auto num_attr_size_1 = duckdb::ListVector::GetListSize(*first_triple_children[1]) / size;
        auto num_attr_size_2 = duckdb::ListVector::GetListSize(*sec_triple_children[1]) / size;

        auto cat_attr_size_1 = duckdb::ListVector::GetListSize(*first_triple_children[3]) / size;
        auto cat_attr_size_2 = duckdb::ListVector::GetListSize(*sec_triple_children[3]) / size;

        auto lin_list_entries_1 = (float *) duckdb::ListVector::GetEntry(
                *first_triple_children[1]).GetData();//entries are float
        auto lin_list_entries_2 = (float *) duckdb::ListVector::GetEntry(
                *sec_triple_children[1]).GetData();//entries are float


        (*result_children[1]).SetVectorType(duckdb::VectorType::FLAT_VECTOR);
        auto meta_lin_num = duckdb::FlatVector::GetData<duckdb::list_entry_t>(*result_children[1]);
        auto meta_lin_cat = duckdb::FlatVector::GetData<duckdb::list_entry_t>(*result_children[3]);

        //creates a single vector
        for (idx_t i = 0; i < size; i++) {
            //add first list for the tuple
            for (idx_t j = 0; j < num_attr_size_1; j++)
                duckdb::ListVector::PushBack(*result_children[1], duckdb::Value(lin_list_entries_1[j + (i * num_attr_size_1)] * (*N_2)));

            for (idx_t j = 0; j < num_attr_size_2; j++)
                duckdb::ListVector::PushBack(*result_children[1], duckdb::Value(lin_list_entries_2[j + (i * num_attr_size_2)] * (*N_1)));
        }

        //auto xx = *first_triple_children[3]
        Vector v_cat_lin_1 = duckdb::ListVector::GetEntry(*first_triple_children[3]);
        Vector v_cat_lin_2 = duckdb::ListVector::GetEntry(*sec_triple_children[3]);
        duckdb::ListVector::Reserve(*result_children[3], (cat_attr_size_1 + cat_attr_size_2) * size);
        duckdb::ListVector::SetListSize(*result_children[3], (cat_attr_size_1 + cat_attr_size_2) * size);
        result_children[3]->SetVectorType(VectorType::FLAT_VECTOR);
        Vector &v_cat_lin_res = duckdb::ListVector::GetEntry(*result_children[3]);

        //set linear categorical
        std::cout<<"set linear categorical\n";

        for (idx_t i = 0; i < size; i++) {
            //add first linears
            for (idx_t column = 0; column < cat_attr_size_1; column++) {
                vector<duckdb::Value> children = duckdb::ListValue::GetChildren(v_cat_lin_1.GetValue(column + (i*cat_attr_size_1)));
                vector<Value> cat_vals = {};
                for (idx_t item = 0; item < children.size(); item++) {
                    child_list_t<Value> struct_values;
                    const vector<Value> &struct_children = duckdb::StructValue::GetChildren(children[item]);
                    struct_values.emplace_back("key", Value(struct_children[0]));
                    struct_values.emplace_back("value", Value(struct_children[1].GetValue<float>() * (*N_2)));
                    cat_vals.push_back(duckdb::Value::STRUCT(struct_values));
                }
                v_cat_lin_res.SetValue(column + (i * (cat_attr_size_1 + cat_attr_size_2)), duckdb::Value::LIST(cat_vals));
            }

            for (idx_t column = 0; column < cat_attr_size_2; column++) {
                const vector<duckdb::Value> children = duckdb::ListValue::GetChildren(v_cat_lin_2.GetValue(column + (i*cat_attr_size_2)));
                vector<Value> cat_vals = {};
                for (idx_t item = 0; item < children.size(); item++) {
                    child_list_t<Value> struct_values;
                    const vector<Value> struct_children = duckdb::StructValue::GetChildren(children[item]);
                    struct_values.emplace_back("key", Value(struct_children[0]));
                    struct_values.emplace_back("value", Value(struct_children[1].GetValue<float>() * (*N_1)));
                    cat_vals.push_back(duckdb::Value::STRUCT(struct_values));
                }
                v_cat_lin_res.SetValue(column + cat_attr_size_1 + (i * (cat_attr_size_1 + cat_attr_size_2)), duckdb::Value::LIST(cat_vals));
            }
        }

        //set linear metadata -> for each row to return the metadata (view of the full vector)
        for (idx_t child_idx = 0; child_idx < size; child_idx++) {
            meta_lin_num[child_idx].length = num_attr_size_1 + num_attr_size_2;
            meta_lin_num[child_idx].offset =
                    child_idx * meta_lin_num[child_idx].length;//ListVector::GetListSize(*result_children[1]);
                    meta_lin_cat[child_idx].length = cat_attr_size_1 + cat_attr_size_2;
            meta_lin_cat[child_idx].offset =
                    child_idx * meta_lin_cat[child_idx].length;//ListVector::GetListSize(*result_children[1]);
        }

        //set quadratic aggregates
        std::cout<<"set quadratic aggregates\n";

        RecursiveFlatten(*first_triple_children[2], size);
                D_ASSERT(first_triple_children[2]->GetVectorType() == duckdb::VectorType::FLAT_VECTOR);
        RecursiveFlatten(*sec_triple_children[2], size);
                D_ASSERT(sec_triple_children[2]->GetVectorType() == duckdb::VectorType::FLAT_VECTOR);

        auto quad_lists_size_1 = duckdb::ListVector::GetListSize(*first_triple_children[2]) / size;
        auto quad_lists_size_2 = duckdb::ListVector::GetListSize(*sec_triple_children[2]) / size;
        auto quad_list_entries_1 = (float *) duckdb::ListVector::GetEntry(
                *first_triple_children[2]).GetData();//entries are float
        auto quad_list_entries_2 = (float *) duckdb::ListVector::GetEntry(
                *sec_triple_children[2]).GetData();//entries are float

        (*result_children[2]).SetVectorType(duckdb::VectorType::FLAT_VECTOR);
        auto meta_quad_num = duckdb::FlatVector::GetData<duckdb::list_entry_t>(*result_children[2]);
        auto meta_quad_num_cat = duckdb::FlatVector::GetData<duckdb::list_entry_t>(*result_children[4]);
        auto meta_quad_cat_cat = duckdb::FlatVector::GetData<duckdb::list_entry_t>(*result_children[5]);

        //compute quad numerical
        std::cout<<"set quad numerical\n";

        //for each row
        for (idx_t i = 0; i < size; i++) {
            //scale 1st quad. aggregate
            auto idx_lin_quad = 0;
            for (idx_t j = 0; j < num_attr_size_1; j++) {
                for (idx_t k = j;
                     k < num_attr_size_1; k++) {//a list * b (AA, BB, <prod. A and cols other table computed down>, AB)
                    duckdb::ListVector::PushBack(*result_children[2],
                                                 duckdb::Value(
                                                         quad_list_entries_1[idx_lin_quad + (i * quad_lists_size_1)] * (*N_2)));
                    idx_lin_quad++;
                }

                //multiply lin. aggregates
                for (idx_t k = 0; k < num_attr_size_2; k++) {
                    duckdb::ListVector::PushBack(*result_children[2],
                                                 duckdb::Value(lin_list_entries_1[j + (i * num_attr_size_1)] *
                                                               lin_list_entries_2[k + (i * num_attr_size_2)]));
                    std::cout<<"Quad NUM: "<<lin_list_entries_1[j + (i * num_attr_size_1)] *
                                             lin_list_entries_2[k + (i * num_attr_size_2)]<<"\n";
                }
            }

            //scale 2nd quad. aggregate
            for (idx_t j = 0; j < quad_lists_size_2; j++) {
                duckdb::ListVector::PushBack(*result_children[2],
                                             duckdb::Value(quad_list_entries_2[j + (i * quad_lists_size_2)] * (*N_1)));
            }
        }

        //set num*cat aggregates
        Vector v_num_cat_quad_1 = duckdb::ListVector::GetEntry(
                *first_triple_children[4]);
        Vector v_num_cat_quad_2 = duckdb::ListVector::GetEntry(
                *sec_triple_children[4]);

        auto size_num_cat_cols = ((num_attr_size_1 * cat_attr_size_1) + (num_attr_size_1 * cat_attr_size_2)
                                  + (num_attr_size_2 * cat_attr_size_1) + (num_attr_size_2 * cat_attr_size_2));

        duckdb::ListVector::Reserve(*result_children[4], size_num_cat_cols * size);
        duckdb::ListVector::SetListSize(*result_children[4], size_num_cat_cols * size);

        result_children[4]->SetVectorType(VectorType::FLAT_VECTOR);
        Vector &v_num_cat_quad_res = duckdb::ListVector::GetEntry(*result_children[4]);

        //new size is (cont_A * cat_A) (curr. size) + (cont_A * cat_B) + (cont_B * cat_A) + (cont_B * cat_B)
        size_t set_index = 0;

        //compute quad num_cat

        for (idx_t i = 0; i < size; i++) {
            //scale entries cont_A * cat_A
            auto row_offset = i*(num_attr_size_1 * cat_attr_size_1);
            for (size_t col1 = 0; col1 < num_attr_size_1; col1++){
                for (size_t col2 = 0; col2 < cat_attr_size_1; col2++){
                    const vector<duckdb::Value> children = duckdb::ListValue::GetChildren(v_num_cat_quad_1.GetValue((col1 * cat_attr_size_1) + col2 + row_offset));
                    vector<Value> cat_vals = {};

                    for(size_t cat_val = 0; cat_val < children.size(); cat_val++){
                        child_list_t<Value> struct_values;
                        const vector<Value> struct_children = duckdb::StructValue::GetChildren(children[cat_val]);
                        struct_values.emplace_back("key", struct_children[0]);
                        struct_values.emplace_back("value", Value(struct_children[1].GetValue<float>() * (*N_2)));
                        cat_vals.push_back(duckdb::Value::STRUCT(struct_values));
                    }
                    v_num_cat_quad_res.SetValue(set_index, duckdb::Value::LIST(cat_vals));
                    set_index++;
                }

                //compute cont_A * cat_B
                for (size_t col2 = 0; col2 < cat_attr_size_2; col2++){
                    const vector<duckdb::Value> children = duckdb::ListValue::GetChildren(v_cat_lin_2.GetValue(col2 + (i*cat_attr_size_2)));
                    vector<Value> cat_vals = {};
                    for(size_t cat_val = 0; cat_val < children.size(); cat_val++){
                        child_list_t<Value> struct_values;
                        const vector<Value> struct_children = duckdb::StructValue::GetChildren(children[cat_val]);
                        struct_values.emplace_back("key", struct_children[0]);
                        struct_values.emplace_back("value", Value(struct_children[1].GetValue<float>() * lin_list_entries_1[col1 + (i*num_attr_size_1)]));
                        cat_vals.push_back(duckdb::Value::STRUCT(struct_values));
                    }
                    v_num_cat_quad_res.SetValue(set_index, duckdb::Value::LIST(cat_vals));
                    set_index++;
                }
            }

            //compute cont_B * cat_A
            for (size_t col1 = 0; col1 < num_attr_size_2; col1++){
                for (size_t col2 = 0; col2 < cat_attr_size_1; col2++){
                    const vector<duckdb::Value> children = duckdb::ListValue::GetChildren(v_cat_lin_1.GetValue(col2 + (i*cat_attr_size_1)));
                    vector<Value> cat_vals = {};
                    for(size_t cat_val = 0; cat_val < children.size(); cat_val++){
                        child_list_t<Value> struct_values;
                        const vector<Value> struct_children = duckdb::StructValue::GetChildren(children[cat_val]);
                        struct_values.emplace_back("key", struct_children[0]);
                        struct_values.emplace_back("value", Value(struct_children[1].GetValue<float>() * lin_list_entries_2[col1 + (i*num_attr_size_2)]));
                        cat_vals.push_back(duckdb::Value::STRUCT(struct_values));
                    }
                    v_num_cat_quad_res.SetValue(set_index, duckdb::Value::LIST(cat_vals));
                    set_index++;
                }

                //scale entries cont_B * cat_B
                row_offset = i*(num_attr_size_2 * cat_attr_size_2);
                for (size_t col2 = 0; col2 < cat_attr_size_2; col2++){
                    const vector<duckdb::Value> children = duckdb::ListValue::GetChildren(v_num_cat_quad_2.GetValue((col1 * cat_attr_size_2) + col2 + row_offset));
                    vector<Value> cat_vals = {};

                    for(size_t cat_val = 0; cat_val < children.size(); cat_val++){
                        child_list_t<Value> struct_values;
                        const vector<Value> struct_children = duckdb::StructValue::GetChildren(children[cat_val]);
                        struct_values.emplace_back("key", struct_children[0]);
                        struct_values.emplace_back("value", Value(struct_children[1].GetValue<float>() * (*N_1)));
                        cat_vals.push_back(duckdb::Value::STRUCT(struct_values));
                    }
                    v_num_cat_quad_res.SetValue(set_index, duckdb::Value::LIST(cat_vals));
                    set_index++;
                }
            }
        }

        //set cat*cat aggregates

        Vector v_cat_cat_quad_1 = duckdb::ListVector::GetEntry(
                *first_triple_children[5]);
        Vector v_cat_cat_quad_2 = duckdb::ListVector::GetEntry(
                *sec_triple_children[5]);
        //new size is old size A + old size B +
        auto size_cat_cat_cols = ((cat_attr_size_1 * (cat_attr_size_1+1)) /2) + ((cat_attr_size_2 * (cat_attr_size_2+1)) /2) + (cat_attr_size_1 * cat_attr_size_2);
        duckdb::ListVector::Reserve(*result_children[5], size_cat_cat_cols * size);
        duckdb::ListVector::SetListSize(*result_children[5], size_cat_cat_cols * size);

        result_children[5]->SetVectorType(VectorType::FLAT_VECTOR);
        Vector &v_cat_cat_quad_res = duckdb::ListVector::GetEntry(*result_children[5]);

        set_index = 0;
        std::cout<<"set cat*cat\n";

        auto size_row = (cat_attr_size_1 * (cat_attr_size_1+1)) /2;
        for (idx_t i = 0; i < size; i++) {
            // (cat_A * cat_A) * count_B (scale cat_A)
            size_t idx_cat_in = 0;
            for (size_t col1 = 0; col1 < cat_attr_size_1; col1++) {
                for (size_t col2 = col1; col2 < cat_attr_size_1; col2++) {
                    const vector<duckdb::Value> children = duckdb::ListValue::GetChildren(
                            v_cat_cat_quad_1.GetValue(idx_cat_in + (i * size_row)));
                    vector<Value> cat_vals = {};
                    for (size_t el = 0; el < children.size(); el++) {
                        child_list_t<Value> struct_values;
                        const vector<Value> struct_children = duckdb::StructValue::GetChildren(children[el]);
                        struct_values.emplace_back("key1", struct_children[0]);
                        struct_values.emplace_back("key2", struct_children[1]);
                        struct_values.emplace_back("value", Value(struct_children[2].GetValue<float>() * (*N_2)));
                        cat_vals.push_back(duckdb::Value::STRUCT(struct_values));
                    }
                    v_cat_cat_quad_res.SetValue(set_index, duckdb::Value::LIST(cat_vals));
                    set_index++;
                    idx_cat_in++;
                }

                //cat A * cat B
                const vector<duckdb::Value> children1 = duckdb::ListValue::GetChildren(v_cat_lin_1.GetValue(col1 + (i*cat_attr_size_1)));
                for (size_t col2 = 0; col2 < cat_attr_size_2; col2++) {
                    const vector<duckdb::Value> children2 = duckdb::ListValue::GetChildren(v_cat_lin_2.GetValue(col2 + (i*cat_attr_size_2)));
                    vector<Value> cat_vals = {};
                    for (size_t j = 0; j < children1.size(); j++){
                        const vector<Value> &struct_children1 = duckdb::StructValue::GetChildren(children1[j]);
                        for (size_t k = 0; k < children2.size(); k++){
                            const vector<Value> struct_children2 = duckdb::StructValue::GetChildren(children2[k]);
                            child_list_t<Value> struct_values;
                            struct_values.emplace_back("key1", struct_children1[0]);
                            struct_values.emplace_back("key2", struct_children2[0]);
                            struct_values.emplace_back("value", Value(struct_children1[1].GetValue<float>() * struct_children2[1].GetValue<float>()));
                            cat_vals.push_back(duckdb::Value::STRUCT(struct_values));
                        }
                    }
                    v_cat_cat_quad_res.SetValue(set_index, duckdb::Value::LIST(cat_vals));
                    set_index++;
                }
            }

            // (cat_B * cat_B) * count_A (scale cat_B)
            size_row = (cat_attr_size_2 * (cat_attr_size_2+1)) /2;
            for (size_t col = 0; col < size_row; col++) {
                const vector<duckdb::Value> children = duckdb::ListValue::GetChildren(v_cat_cat_quad_2.GetValue(col + (i*size_row)));
                vector<Value> cat_vals = {};
                for (size_t el = 0; el < children.size(); el++){
                    child_list_t<Value> struct_values;
                    const vector<Value> struct_children = duckdb::StructValue::GetChildren(children[el]);
                    struct_values.emplace_back("key1", struct_children[0]);
                    struct_values.emplace_back("key2", struct_children[1]);
                    struct_values.emplace_back("value", Value(struct_children[2].GetValue<float>() * (*N_1)));
                    cat_vals.push_back(duckdb::Value::STRUCT(struct_values));
                }
                v_cat_cat_quad_res.SetValue(set_index, duckdb::Value::LIST(cat_vals));
                set_index++;
            }
        }

        //set for each row to return the metadata (view of the full vector)
        for (idx_t row = 0; row < size; row++) {
            meta_quad_num[row].length = ((num_attr_size_1 * (num_attr_size_1 + 1))/2) + (num_attr_size_1 * num_attr_size_2) + ((num_attr_size_2 * (num_attr_size_2 + 1))/2);
            meta_quad_num[row].offset = row * meta_quad_num[row].length;

            meta_quad_num_cat[row].length = size_num_cat_cols;
            meta_quad_num_cat[row].offset = row * meta_quad_num_cat[row].length;

            meta_quad_cat_cat[row].length = size_cat_cat_cols;
            meta_quad_cat_cat[row].offset = row * meta_quad_cat_cat[row].length;
        }
    }

    //Returns the datatype used by this function
    duckdb::unique_ptr<duckdb::FunctionData>
    MultiplyBind(duckdb::ClientContext &context, duckdb::ScalarFunction &function,
                 duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &arguments) {

                D_ASSERT(arguments.size() == 2);
        //function.return_type = arguments[0]->return_type;

        child_list_t<LogicalType> struct_children;
        struct_children.emplace_back("N", LogicalType::INTEGER);
        struct_children.emplace_back("lin_num", LogicalType::LIST(LogicalType::FLOAT));
        struct_children.emplace_back("quad_num", LogicalType::LIST(LogicalType::FLOAT));

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
        //lin_cat -> LIST(LIST(STRUCT(key1, key2, val))). E.g. [[{k1,k2,2},{k3,k4,5}],[]]...
        //quad_cat

        auto struct_type = LogicalType::STRUCT(struct_children);
        function.return_type = struct_type;


        return duckdb::make_uniq<duckdb::VariableReturnBindData>(function.return_type);
    }

    //Generate statistics for this function. Given input type ststistics (mainly min and max for every attribute), returns the output statistics
    duckdb::unique_ptr<duckdb::BaseStatistics>
    StructPackStats(duckdb::ClientContext &context, duckdb::FunctionStatisticsInput &input) {
        std::cout << "StructPackStats " << std::endl;
        auto &child_stats = input.child_stats;
        std::cout << "StructPackStats " << child_stats.size() << child_stats[0].ToString() << std::endl;
        auto &expr = input.expr;
        auto struct_stats = duckdb::StructStats::CreateUnknown(expr.return_type);
        /*
        duckdb::StructStats::Copy(struct_stats, child_stats[0]);
        duckdb::BaseStatistics &ret_num_stats = duckdb::StructStats::GetChildStats(struct_stats, 0);

        //set statistics for N
        if (!duckdb::NumericStats::HasMinMax(ret_num_stats) || !duckdb::NumericStats::HasMinMax(duckdb::StructStats::GetChildStats(child_stats[1], 0)))
                return struct_stats.ToUnique();


        int32_t n_1_min = duckdb::NumericStats::Min(ret_num_stats).GetValue<int32_t>();
        int32_t n_1_max = duckdb::NumericStats::Max(ret_num_stats).GetValue<int32_t>();

        int32_t n_2_min = duckdb::NumericStats::GetMin<int32_t>(duckdb::StructStats::GetChildStats(child_stats[1], 0));
        int32_t n_2_max = duckdb::NumericStats::GetMax<int32_t>(duckdb::StructStats::GetChildStats(child_stats[1], 0));

        duckdb::NumericStats::SetMax(ret_num_stats, duckdb::Value(n_1_max * n_2_max));
        duckdb::NumericStats::SetMin(ret_num_stats, duckdb::Value(n_1_min * n_2_min));

        //set statistics for lin. aggregates
        duckdb::BaseStatistics& list_num_stat = duckdb::ListStats::GetChildStats(duckdb::StructStats::GetChildStats(struct_stats, 1));

        if (!duckdb::NumericStats::HasMinMax(list_num_stat) || !duckdb::NumericStats::HasMinMax(duckdb::ListStats::GetChildStats(duckdb::StructStats::GetChildStats(child_stats[1], 1))))
            return struct_stats.ToUnique();

        float lin_1_min = duckdb::NumericStats::GetMin<float>(list_num_stat);
        float lin_1_max = duckdb::NumericStats::GetMax<float>(list_num_stat);

        float lin_2_min = duckdb::NumericStats::GetMin<float>(duckdb::ListStats::GetChildStats(duckdb::StructStats::GetChildStats(child_stats[1], 1)));
        float lin_2_max = duckdb::NumericStats::GetMax<float>(duckdb::ListStats::GetChildStats(duckdb::StructStats::GetChildStats(child_stats[1], 1)));

        int32_t n_1_min_n = n_1_min;
        int32_t n_1_max_n = n_1_max;
        int32_t n_2_min_n = n_2_min;
        int32_t n_2_max_n = n_2_max;

        if (lin_1_max < 0){
            int tmp = n_2_max_n;
            n_2_max_n = n_2_min_n;
            n_2_min_n = tmp;
        }
        else if (lin_1_min < 0){//& max > 0
            n_2_min_n = n_2_max_n;
        }
        if (lin_2_max < 0){
            int tmp = n_1_max_n;
            n_1_max_n = n_1_min_n;
            n_1_min_n = tmp;
        }
        else if (lin_2_min < 0){//& max > 0
            n_1_min_n = n_1_max_n;
        }

        duckdb::NumericStats::SetMax(list_num_stat, duckdb::Value(std::max(n_2_max_n * lin_1_max, n_1_max_n* lin_2_max)));//n max and min are the same

        duckdb::NumericStats::SetMin(list_num_stat, duckdb::Value(std::min(n_2_min_n * lin_1_min, n_1_min_n * lin_2_min)));

        //set statistics for quadratic aggregate
        duckdb::BaseStatistics& list_quad_stat = duckdb::ListStats::GetChildStats(duckdb::StructStats::GetChildStats(struct_stats, 2));

        //max value in quadratic aggreagate is max of linear product, scale first list and scale second list
        if (!duckdb::NumericStats::HasMinMax(list_quad_stat) || duckdb::NumericStats::HasMinMax(duckdb::ListStats::GetChildStats(duckdb::StructStats::GetChildStats(child_stats[1], 2))))
            return struct_stats.ToUnique();

        n_1_min_n = n_1_min;
        n_1_max_n = n_1_max;
        n_2_min_n = n_2_min;
        n_2_max_n = n_2_max;

        if (duckdb::NumericStats::GetMax<float>(list_quad_stat) < 0){
            int tmp = n_2_max_n;
            n_2_max_n = n_2_min_n;
            n_2_min_n = tmp;
        }
        else if (duckdb::NumericStats::GetMin<float>(list_quad_stat) < 0){
            n_2_min_n = n_2_max_n;
        }
        if (duckdb::NumericStats::GetMax<float>(duckdb::ListStats::GetChildStats(duckdb::StructStats::GetChildStats(child_stats[1], 2))) < 0){
            int tmp = n_1_max_n;
            n_1_max_n = n_1_min_n;
            n_1_min_n = tmp;
        }
        else if (duckdb::NumericStats::GetMin<float>(duckdb::ListStats::GetChildStats(duckdb::StructStats::GetChildStats(child_stats[1], 2))) < 0){
            n_1_min_n = n_1_max_n;
        }


        duckdb::NumericStats::SetMax(list_quad_stat, duckdb::Value(std::max(std::max(std::max(std::max(lin_1_max * lin_2_max, lin_1_min * lin_2_min), lin_1_max*lin_2_min), lin_2_max*lin_1_min),
                                                                            std::max(n_2_max_n * duckdb::NumericStats::GetMax<float>(list_quad_stat),
                                                                           n_1_max_n * duckdb::NumericStats::GetMax<float>(duckdb::ListStats::GetChildStats(duckdb::StructStats::GetChildStats(child_stats[1], 2)))))));

        duckdb::NumericStats::SetMin(list_quad_stat, duckdb::Value(std::max(std::max(std::max(std::max(lin_1_max * lin_2_max, lin_1_min * lin_2_min), lin_1_max*lin_2_min), lin_2_max*lin_1_min),
                                                                            std::min(n_2_min_n * duckdb::NumericStats::GetMin<float>(list_quad_stat),
                                                                                     n_1_min_n * duckdb::NumericStats::GetMin<float>(duckdb::ListStats::GetChildStats(duckdb::StructStats::GetChildStats(child_stats[1], 2)))))));

        std::cout << "statistics StructPackStats" << struct_stats.ToString() << std::endl;
         */
        return struct_stats.ToUnique();
    }
}