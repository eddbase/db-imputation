//
// Created by Massimo Perini on 02/06/2023.
//

#include "Custom_lift.h"
#include "From_duckdb.h"
#include <duckdb/function/scalar/nested_functions.hpp>

#include <iostream>


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

        size_t num_cols = 0;
        size_t cat_cols = 0;

        for (idx_t j=0;j<columns;j++){
            auto col_type = in_data[j].GetType();
            if (col_type == LogicalType::FLOAT || col_type == LogicalType::DOUBLE)
                num_cols++;
            else
                cat_cols++;
        }

        //set N

        auto N_vec = (int32_t *) duckdb::FlatVector::GetData(*result_children[0]);//first child (N)

        for (idx_t i = 0; i < size; i++) {
            N_vec[i] = 1;
        }

        //set linear

        //get result structs for numerical
        duckdb::ListVector::Reserve(*result_children[1], num_cols*size);
        duckdb::ListVector::SetListSize(*result_children[1], num_cols*size);
        auto lin_vec_num = duckdb::FlatVector::GetData<duckdb::list_entry_t>(*result_children[1]);//sec child (lin. aggregates)
        auto lin_vec_num_data = (float *) duckdb::ListVector::GetEntry(*result_children[1]).GetData();

        //get result struct for categorical
        duckdb::ListVector::Reserve(*result_children[3], cat_cols*size);
        duckdb::ListVector::SetListSize(*result_children[3], cat_cols*size);
        auto lin_vec_cat = duckdb::FlatVector::GetData<duckdb::list_entry_t>(*result_children[3]);
        Vector &cat_relations_vector = duckdb::ListVector::GetEntry(*result_children[3]);

        size_t curr_numerical = 0;
        size_t curr_categorical = 0;

        for (idx_t j=0;j<columns;j++){
            auto col_type = in_data[j].GetType();
            if (col_type == LogicalType::FLOAT || col_type == LogicalType::DOUBLE) {
                float *column_data = (float *)in_data[j].GetData();//input column
                for (idx_t i = 0; i < size; i++) {
                    lin_vec_num_data[curr_numerical + (i * num_cols)] = column_data[i];
                }
                curr_numerical++;
            }
            else{//empty relations
                int *column_data = (int *)in_data[j].GetData();//input column
                for (idx_t i = 0; i < size; i++) {
                    vector<Value> cat_vals = {};
                    child_list_t<Value> struct_values;
                    struct_values.emplace_back("key", Value(column_data[i]));
                    struct_values.emplace_back("value", Value(1));
                    cat_vals.push_back(duckdb::Value::STRUCT(struct_values));
                    cat_relations_vector.SetValue(curr_categorical + (i * cat_cols), duckdb::Value::LIST(cat_vals));
                }
                //duckdb::ListVector::PushBack(*result_children[3], duckdb::Value::LIST(cat_vals));
                curr_categorical++;
            }
        }
        std::cout<<"cat linear: "<<duckdb::ListVector::GetListSize(*result_children[1])<<"\n";

        //set N*N
        //std::cout<<"N*N"<<std::endl;
        duckdb::ListVector::Reserve(*result_children[2], ((num_cols*(num_cols+1))/2) * size);
        duckdb::ListVector::SetListSize(*result_children[2], ((num_cols*(num_cols+1))/2) * size);
        auto quad_vec = duckdb::FlatVector::GetData<duckdb::list_entry_t>(*result_children[2]);//sec child (lin. aggregates)
        auto quad_vec_data = (float *) duckdb::ListVector::GetEntry(*result_children[2]).GetData();

        //numerical * numerical
        int col_idx = 0;
        for (idx_t j=0;j<columns;j++){
            auto col_type = in_data[j].GetType();
            if (col_type == LogicalType::FLOAT || col_type == LogicalType::DOUBLE) {
                float *column_data = (float *) in_data[j].GetData();//input column //todo potential problems
                for (idx_t k = j; k < columns; k++) {
                    auto col_type = in_data[k].GetType();
                    if (col_type == LogicalType::FLOAT || col_type == LogicalType::DOUBLE) {
                        float *sec_column_data = (float *) in_data[k].GetData();//numerical * numerical
                        for (idx_t i = 0; i < size; i++) {
                            quad_vec_data[col_idx + (i * num_cols * (num_cols + 1) / 2)] =
                                    column_data[i] * sec_column_data[i];
                        }
                        col_idx++;
                    }
                }
            }
        }
        std::cout<<"num quad: "<<duckdb::ListVector::GetListSize(*result_children[2])<<"\n";


        duckdb::ListVector::Reserve(*result_children[4], (num_cols * cat_cols) * size);
        duckdb::ListVector::SetListSize(*result_children[4], (num_cols * cat_cols) * size);
        Vector &cat_relations_vector_num_quad = duckdb::ListVector::GetEntry(*result_children[4]);
        auto cat_relations_vector_num_quad_list = duckdb::FlatVector::GetData<duckdb::list_entry_t>(*result_children[4]);
        //num * categorical
        col_idx = 0;
        for (idx_t j=0;j<columns;j++){
            auto col_type = in_data[j].GetType();
            if (col_type == LogicalType::FLOAT || col_type == LogicalType::DOUBLE) {
                //numerical column
                float *num_column_data = (float *) in_data[j].GetData();//col1
                for (idx_t k = 0; k < columns; k++) {
                    auto col_type = in_data[k].GetType();
                    if (col_type != LogicalType::FLOAT && col_type != LogicalType::DOUBLE) {//numerical * categorical
                        int *cat_column_data = (int *) in_data[k].GetData();//categorical
                        for (idx_t i = 0; i < size; i++) {
                            vector<Value> cat_vals = {};
                            child_list_t<Value> struct_values;
                            struct_values.emplace_back("key1", Value(cat_column_data[i]));
                            struct_values.emplace_back("value", Value(num_column_data[i]));
                            cat_vals.push_back(duckdb::Value::STRUCT(struct_values));
                            cat_relations_vector_num_quad.SetValue(col_idx + (i * (cat_cols * num_cols)), duckdb::Value::LIST(cat_vals));
                        }
                        col_idx++;
                    }
                }
            }
        }
        std::cout<<"num*cat: "<<duckdb::ListVector::GetListSize(*result_children[4])<<"\n";

        //categorical * categorical

        duckdb::ListVector::Reserve(*result_children[5], ((cat_cols*(cat_cols+1))/2) * size);
        duckdb::ListVector::SetListSize(*result_children[5], ((cat_cols*(cat_cols+1))/2) * size);
        Vector &cat_relations_vector_cat_quad = duckdb::ListVector::GetEntry(*result_children[5]);
        auto cat_relations_vector_cat_quad_list = duckdb::FlatVector::GetData<duckdb::list_entry_t>(*result_children[5]);

        col_idx = 0;
        for (idx_t j=0;j<columns;j++){
            auto col_type = in_data[j].GetType();
            if (col_type != LogicalType::FLOAT && col_type != LogicalType::DOUBLE) {
                int *cat_1 = (int *) in_data[j].GetData();//col1
                for (idx_t k = j; k < columns; k++) {
                    auto col_type = in_data[k].GetType();
                    if (col_type != LogicalType::FLOAT && col_type != LogicalType::DOUBLE) {//categorical * categorical
                        int *cat_2 = (int *) in_data[k].GetData();//categorical
                        for (idx_t i = 0; i < size; i++) {
                            vector<Value> cat_vals = {};
                            child_list_t<Value> struct_values;
                            struct_values.emplace_back("key1", Value(cat_1[i]));
                            struct_values.emplace_back("key2", Value(cat_2[i]));
                            struct_values.emplace_back("value", Value(1));
                            cat_vals.push_back(duckdb::Value::STRUCT(struct_values));
                            cat_relations_vector_cat_quad.SetValue(col_idx + (i * ((cat_cols * (cat_cols + 1)) / 2)), duckdb::Value::LIST(cat_vals));
                        }
                        col_idx++;
                    }
                }
            }
        }

        std::cout<<"cat*cat: "<<duckdb::ListVector::GetListSize(*result_children[5])<<"\n";

        //std::cout<<"set views"<<std::endl;
        //set views
        for(int i=0;i<size;i++) {
            lin_vec_num[i].length = num_cols;
            lin_vec_num[i].offset = i * lin_vec_num[i].length;

            lin_vec_cat[i].length = cat_cols;
            lin_vec_cat[i].offset = i * lin_vec_cat[i].length;

            quad_vec[i].length = num_cols*(num_cols+1)/2;
            quad_vec[i].offset = i * quad_vec[i].length;

            cat_relations_vector_num_quad_list[i].length = num_cols*cat_cols;
            cat_relations_vector_num_quad_list[i].offset = i * cat_relations_vector_num_quad_list[i].length;

            cat_relations_vector_cat_quad_list[i].length = cat_cols*(cat_cols+1)/2;
            cat_relations_vector_cat_quad_list[i].offset = i * cat_relations_vector_cat_quad_list[i].length;


        }
        //set categorical
    }

    //Returns the datatype used by this function
    duckdb::unique_ptr<duckdb::FunctionData>
    CustomLiftBind(duckdb::ClientContext &context, duckdb::ScalarFunction &function,
                 duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &arguments) {

        //set return type

        child_list_t<LogicalType> struct_children;
        struct_children.emplace_back("N", LogicalType::INTEGER);
        struct_children.emplace_back("lin_num", LogicalType::LIST(LogicalType::FLOAT));
        struct_children.emplace_back("quad_num", LogicalType::LIST(LogicalType::FLOAT));

        //categorical structures
        child_list_t<LogicalType> lin_cat;
        lin_cat.emplace_back("key", LogicalType::INTEGER);
        lin_cat.emplace_back("value", LogicalType::INTEGER);

        struct_children.emplace_back("lin_cat", LogicalType::LIST(LogicalType::LIST(LogicalType::STRUCT(lin_cat))));

        child_list_t<LogicalType> quad_num_cat;
        quad_num_cat.emplace_back("key1", LogicalType::INTEGER);
        quad_num_cat.emplace_back("value", LogicalType::INTEGER);
        struct_children.emplace_back("quad_num_cat", LogicalType::LIST(LogicalType::LIST(LogicalType::STRUCT(quad_num_cat))));

        child_list_t<LogicalType> quad_cat_cat;
        quad_cat_cat.emplace_back("key1", LogicalType::INTEGER);
        quad_cat_cat.emplace_back("key2", LogicalType::INTEGER);
        quad_cat_cat.emplace_back("value", LogicalType::INTEGER);
        struct_children.emplace_back("quad_cat", LogicalType::LIST(LogicalType::LIST(LogicalType::STRUCT(quad_cat_cat))));
        //lin_cat -> LIST(LIST(STRUCT(key1, key2, val))). E.g. [[{k1,k2,2},{k3,k4,5}],[]]...
        //quad_cat

        auto struct_type = LogicalType::STRUCT(struct_children);
        function.return_type = struct_type;
        //set arguments
        function.varargs = LogicalType::ANY;
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