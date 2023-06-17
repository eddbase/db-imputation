//
// Created by Massimo Perini on 14/06/2023.
//

#include "helper.h"

#include "Triple_sum.h"
#include "Triple_mul.h"
#include "Lift.h"
#include "Custom_lift.h"
#include "Sum_no_lift.h"
#include "Triple_sub.h"

#include <duckdb/function/scalar/nested_functions.hpp>
#include <duckdb/function/aggregate_function.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace Triple {

    void register_functions(duckdb::ClientContext &context, size_t n_con_columns, size_t n_cat_columns){

        auto function = duckdb::AggregateFunction("triple_sum", {duckdb::LogicalType::ANY}, duckdb::LogicalTypeId::STRUCT, duckdb::AggregateFunction::StateSize<Triple::AggState>,
                                                  duckdb::AggregateFunction::StateInitialize<Triple::AggState, Triple::StateFunction>, Triple::ListUpdateFunction,
                                                  Triple::ListCombineFunction, Triple::ListFinalize, nullptr, Triple::ListBindFunction,
                                                  duckdb::AggregateFunction::StateDestroy<Triple::AggState, Triple::StateFunction>, nullptr, nullptr);

        duckdb::UDFWrapper::RegisterAggrFunction(function, context);
        duckdb::vector<duckdb::LogicalType> args_sum_no_lift = {};
        for(int i=0; i < n_con_columns; i++)
            args_sum_no_lift.push_back(duckdb::LogicalType::FLOAT);
        for(int i=0; i < n_cat_columns; i++)
            args_sum_no_lift.push_back(duckdb::LogicalType::INTEGER);

        auto sum_no_lift = duckdb::AggregateFunction("triple_sum_no_lift", args_sum_no_lift, duckdb::LogicalTypeId::STRUCT, duckdb::AggregateFunction::StateSize<Triple::AggState>,
                                                     duckdb::AggregateFunction::StateInitialize<Triple::AggState, Triple::StateFunction>, Triple::SumNoLift,
                                                     Triple::SumNoLiftCombine, Triple::SumNoLiftFinalize, nullptr, Triple::SumNoLiftBind,
                                                     duckdb::AggregateFunction::StateDestroy<Triple::AggState, Triple::StateFunction>, nullptr, nullptr);
        sum_no_lift.null_handling = duckdb::FunctionNullHandling::SPECIAL_HANDLING;
        sum_no_lift.varargs = duckdb::LogicalType::FLOAT;
        duckdb::UDFWrapper::RegisterAggrFunction(sum_no_lift, context, duckdb::LogicalType::FLOAT);

        //define multiply func.

        duckdb::ScalarFunction fun("multiply_triple", {duckdb::LogicalType::ANY}, duckdb::LogicalTypeId::STRUCT, Triple::StructPackFunction, Triple::MultiplyBind, nullptr,
                                   Triple::StructPackStats);
        fun.varargs = duckdb::LogicalType::ANY;
        fun.null_handling = duckdb::FunctionNullHandling::SPECIAL_HANDLING;
        fun.serialize = duckdb::VariableReturnBindData::Serialize;
        fun.deserialize = duckdb::VariableReturnBindData::Deserialize;
        duckdb::CreateScalarFunctionInfo info(fun);
        info.schema = DEFAULT_SCHEMA;
        context.RegisterFunction(info);

        //sub triples

        duckdb::child_list_t<duckdb::LogicalType> struct_children;
        struct_children.emplace_back("N", duckdb::LogicalType::INTEGER);
        struct_children.emplace_back("lin_agg", duckdb::LogicalType::LIST(duckdb::LogicalType::FLOAT));
        struct_children.emplace_back("quad_agg", duckdb::LogicalType::LIST(duckdb::LogicalType::FLOAT));

        auto struct_type = duckdb::LogicalType::STRUCT(struct_children);

        duckdb::ScalarFunction fun_sub("sub_triple", {struct_type, struct_type}, duckdb::LogicalTypeId::STRUCT, Triple::TripleSubFunction, Triple::TripleSubBind, nullptr,
                                       Triple::TripleSubStats);
        fun_sub.varargs = struct_type;
        fun_sub.null_handling = duckdb::FunctionNullHandling::SPECIAL_HANDLING;
        fun_sub.serialize = duckdb::VariableReturnBindData::Serialize;
        fun_sub.deserialize = duckdb::VariableReturnBindData::Deserialize;
        duckdb::CreateScalarFunctionInfo info_sub(fun_sub);
        info_sub.schema = DEFAULT_SCHEMA;
        context.RegisterFunction(info_sub);


        //custom lift

        duckdb::ScalarFunction custom_lift("lift", {}, duckdb::LogicalTypeId::STRUCT, Triple::CustomLift, Triple::CustomLiftBind, nullptr,
                                           Triple::CustomLiftStats);
        custom_lift.varargs = duckdb::LogicalType::ANY;
        custom_lift.null_handling = duckdb::FunctionNullHandling::SPECIAL_HANDLING;
        custom_lift.serialize = duckdb::VariableReturnBindData::Serialize;
        custom_lift.deserialize = duckdb::VariableReturnBindData::Deserialize;
        duckdb::CreateScalarFunctionInfo custom_lift_info(custom_lift);
        info.schema = DEFAULT_SCHEMA;
        context.RegisterFunction(custom_lift_info);
        //
        //Define update hack function
        /*
        duckdb::vector<duckdb::LogicalType> args_update = {};
        args_update.push_back(duckdb::LogicalType::BOOLEAN);
        for(int i=0; i < n_con_columns; i++)
            args_update.push_back(duckdb::LogicalType::FLOAT);

        duckdb::ScalarFunction update("update", args_update, duckdb::LogicalType::FLOAT, Triple::ImputeHackFunction, Triple::ImputeHackBind, nullptr,
                                      Triple::ImputeHackStats);
        update.null_handling = duckdb::FunctionNullHandling::SPECIAL_HANDLING;
        update.serialize = duckdb::VariableReturnBindData::Serialize;
        update.deserialize = duckdb::VariableReturnBindData::Deserialize;
        duckdb::CreateScalarFunctionInfo update_info(update);
        info.schema = DEFAULT_SCHEMA;
        context.RegisterFunction(update_info);
        */

    }

} // Triple