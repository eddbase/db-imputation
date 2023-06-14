//
// Created by Massimo Perini on 02/06/2023.
//

#include "Custom_lift.h"
#include <iostream>
#include <algorithm>
#include "From_duckdb.h"
#include <duckdb/function/scalar/nested_functions.hpp>


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


namespace Triple {
    //actual implementation of this function
    void CustomLift(duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result) {
        //std::cout << "StructPackFunction start " << std::endl;
        auto &result_children = duckdb::StructVector::GetEntries(result);
        idx_t size = args.size();//n. of rows to return

        vector<Vector> &in_data = args.data;
        int columns = in_data.size();


        //set N

        auto N_vec = (int32_t *) duckdb::FlatVector::GetData(*result_children[0]);//first child (N)

        for (idx_t i = 0; i < size; i++) {
            N_vec[i] = 1;
        }

        //set linear

        duckdb::ListVector::Reserve(*result_children[1], columns*size);
        auto lin_vec = duckdb::FlatVector::GetData<duckdb::list_entry_t>(*result_children[1]);//sec child (lin. aggregates)
        auto lin_vec_data = (float *) duckdb::ListVector::GetEntry(*result_children[1]).GetData();

        for (idx_t j=0;j<columns;j++){
            float *column_data = (float *)in_data[j].GetData();//input column //todo potential problems
            for (idx_t i = 0; i < size; i++) {
                lin_vec_data[j + (i*columns)] = column_data[i];
            }
        }

        //set N*N
        //std::cout<<"N*N"<<std::endl;
        duckdb::ListVector::Reserve(*result_children[2], ((columns*(columns+1))/2) * size);
        auto quad_vec = duckdb::FlatVector::GetData<duckdb::list_entry_t>(*result_children[2]);//sec child (lin. aggregates)
        auto quad_vec_data = (float *) duckdb::ListVector::GetEntry(*result_children[2]).GetData();

        int col_idx = 0;
        for (idx_t j=0;j<columns;j++){
            float *column_data = (float *)in_data[j].GetData();//input column //todo potential problems
            for (idx_t k=j;k<columns;k++){
                float *sec_column_data = (float *)in_data[k].GetData();
                for (idx_t i = 0; i < size; i++) {
                    quad_vec_data[col_idx + (i*columns*(columns+1)/2)] = column_data[i] * sec_column_data[i];
                }
                col_idx++;
            }
        }
        //std::cout<<"set views"<<std::endl;
        //set views
        for(int i=0;i<size;i++) {
            lin_vec[i].length = columns;
            lin_vec[i].offset = i * lin_vec[i].length;

            quad_vec[i].length = columns*(columns+1)/2;
            quad_vec[i].offset = i * quad_vec[i].length;
        }
        //std::cout<<"end"<<std::endl;
    }

    //Returns the datatype used by this function
    duckdb::unique_ptr<duckdb::FunctionData>
    CustomLiftBind(duckdb::ClientContext &context, duckdb::ScalarFunction &function,
                 duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &arguments) {

        //set return type

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

    //Generate statistics for this function. Given input type ststistics (mainly min and max for every attribute), returns the output statistics
    duckdb::unique_ptr<duckdb::BaseStatistics>
    CustomLiftStats(duckdb::ClientContext &context, duckdb::FunctionStatisticsInput &input) {
        auto &child_stats = input.child_stats;
        auto &expr = input.expr;
        auto struct_stats = duckdb::StructStats::CreateUnknown(expr.return_type);
        return struct_stats.ToUnique();
    }
}