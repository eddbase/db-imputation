//
// Created by Massimo Perini on 30/05/2023.
//

#include "Triple_mul.h"
#include <duckdb/function/scalar/nested_functions.hpp>

#include <iostream>
#include <algorithm>
#include "From_duckdb.h"

//DataChunk is a set of vectors

class duckdb::BoundFunctionExpression : public duckdb::Expression {
public:
    static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_FUNCTION;

public:
    BoundFunctionExpression(LogicalType return_type, ScalarFunction bound_function,
                            vector<unique_ptr<Expression>> arguments, unique_ptr<FunctionData> bind_info,
                            bool is_operator = false);

    //! The bound function expression
    ScalarFunction function;
    //! List of child-expressions of the function
    vector<unique_ptr<Expression>> children;
    //! The bound function data (if any)
    unique_ptr<FunctionData> bind_info;
    //! Whether or not the function is an operator, only used for rendering
    bool is_operator;

public:
    bool HasSideEffects() const override;
    bool IsFoldable() const override;
    string ToString() const override;
    bool PropagatesNullValues() const override;
    hash_t Hash() const override;
    bool Equals(const BaseExpression *other) const;

    unique_ptr<Expression> Copy() override;
    void Verify() const override;

    void Serialize(FieldWriter &writer) const override;
    static unique_ptr<Expression> Deserialize(ExpressionDeserializationState &state, FieldReader &reader);
};

namespace duckdb {
    struct ListStats {
        DUCKDB_API static void Construct(BaseStatistics &stats);

        DUCKDB_API static BaseStatistics CreateUnknown(LogicalType type);

        DUCKDB_API static BaseStatistics CreateEmpty(LogicalType type);

        DUCKDB_API static const BaseStatistics &GetChildStats(const BaseStatistics &stats);

        DUCKDB_API static BaseStatistics &GetChildStats(BaseStatistics &stats);

        DUCKDB_API static void SetChildStats(BaseStatistics &stats, unique_ptr<BaseStatistics> new_stats);

        DUCKDB_API static void Serialize(const BaseStatistics &stats, FieldWriter &writer);

        DUCKDB_API static BaseStatistics Deserialize(FieldReader &reader, LogicalType type);

        DUCKDB_API static string ToString(const BaseStatistics &stats);

        DUCKDB_API static void Merge(BaseStatistics &stats, const BaseStatistics &other);

        DUCKDB_API static void Copy(BaseStatistics &stats, const BaseStatistics &other);

        DUCKDB_API static void
        Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count);
    };
}

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

        RecursiveFlatten(*first_triple_children[1], size);
                D_ASSERT((*first_triple_children[1]).GetVectorType() == duckdb::VectorType::FLAT_VECTOR);
        RecursiveFlatten(*sec_triple_children[1], size);
                D_ASSERT((*sec_triple_children[1]).GetVectorType() == duckdb::VectorType::FLAT_VECTOR);

        auto attr_size_1 = duckdb::ListVector::GetListSize(*first_triple_children[1]) / size;
        auto attr_size_2 = duckdb::ListVector::GetListSize(*sec_triple_children[1]) / size;
        auto lin_list_entries_1 = (float *) duckdb::ListVector::GetEntry(
                *first_triple_children[1]).GetData();//entries are float
        auto lin_list_entries_2 = (float *) duckdb::ListVector::GetEntry(
                *sec_triple_children[1]).GetData();//entries are float


        (*result_children[1]).SetVectorType(duckdb::VectorType::FLAT_VECTOR);
        auto result_data = duckdb::FlatVector::GetData<duckdb::list_entry_t>(*result_children[1]);

        //creates a single vector
        for (idx_t i = 0; i < size; i++) {
            //add first list for the tuple
            for (idx_t j = 0; j < attr_size_1; j++)
                duckdb::ListVector::PushBack(*result_children[1], duckdb::Value(lin_list_entries_1[j + (i * attr_size_1)] * (*N_2)));

            for (idx_t j = 0; j < attr_size_2; j++)
                duckdb::ListVector::PushBack(*result_children[1], duckdb::Value(lin_list_entries_2[j + (i * attr_size_2)] * (*N_1)));
        }

        //set for each row to return the metadata (view of the full vector)
        for (idx_t child_idx = 0; child_idx < size; child_idx++) {
            result_data[child_idx].length = attr_size_1 + attr_size_2;
            result_data[child_idx].offset =
                    child_idx * result_data[child_idx].length;//ListVector::GetListSize(*result_children[1]);
        }

        //set quadratic aggregates

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
        result_data = duckdb::FlatVector::GetData<duckdb::list_entry_t>(*result_children[2]);

        //for each row
        for (idx_t i = 0; i < size; i++) {
            //scale 1st quad. aggregate
            for (idx_t j = 0; j < attr_size_1; j++) {
                for (idx_t k = j;
                     k < attr_size_1; k++)//a list * b (AA, BB, <prod. A and cols other table computed down>, AB)
                    duckdb::ListVector::PushBack(*result_children[2],
                                                 duckdb::Value(quad_list_entries_1[j + k + (i * attr_size_1)] * (*N_2)));

                //multiply lin. aggregates
                for (idx_t k = 0; k < attr_size_2; k++)
                    duckdb::ListVector::PushBack(*result_children[2], duckdb::Value(lin_list_entries_1[j + (i * attr_size_1)] *
                                                                    lin_list_entries_2[k + (i * attr_size_2)]));
            }

            //scale 2nd quad. aggregate
            for (idx_t j = 0; j < quad_lists_size_2; j++) {
                duckdb::ListVector::PushBack(*result_children[2],
                                             duckdb::Value(quad_list_entries_2[j + (i * quad_lists_size_2)] * (*N_1)));
                //std::cout << "value to quadratic: " << quad_list_entries_2[j + (i * quad_lists_size_2)] * (*N_1)
                //          << std::endl;
            }

        }

        //set for each row to return the metadata (view of the full vector)
        for (idx_t child_idx = 0; child_idx < size; child_idx++) {
            result_data[child_idx].length = (quad_lists_size_1 + quad_lists_size_2 + (attr_size_1 * attr_size_2));
            result_data[child_idx].offset = child_idx * result_data[child_idx].length;
        }

        //std::cout << "StructPackFunction end " << std::endl;
    }

    //Returns the datatype used by this function
    duckdb::unique_ptr<duckdb::FunctionData>
    MultiplyBind(duckdb::ClientContext &context, duckdb::ScalarFunction &function,
                 duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &arguments) {

                D_ASSERT(arguments.size() == 2);
        function.return_type = arguments[0]->return_type;
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
        return struct_stats.ToUnique();
    }
}