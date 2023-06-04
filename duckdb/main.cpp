#include "lib/libduckdb/src/duckdb.hpp"
#include "triple/Triple_sum.h"
#include "triple/Triple_mul.h"
#include "triple/Lift.h"
#include "triple/Custom_lift.h"
#include "triple/Sum_no_lift.h"

#include "triple/From_duckdb.h"
#include <chrono>

#include <iostream>

struct duckdb::CreateScalarFunctionInfo : public duckdb::CreateFunctionInfo {
    DUCKDB_API explicit CreateScalarFunctionInfo(duckdb::ScalarFunction function);
    DUCKDB_API explicit CreateScalarFunctionInfo(duckdb::ScalarFunctionSet set);

    duckdb::ScalarFunctionSet functions;

public:
    DUCKDB_API duckdb::unique_ptr<duckdb::CreateInfo> Copy() const override;
    DUCKDB_API duckdb::unique_ptr<duckdb::AlterInfo> GetAlterInfo() const override;
};

int main(int argc, char* argv[]) {
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);

    std::vector<string> col_lifted = {"CRS_DEP_HOUR", "CRS_DEP_MIN", "CRS_ARR_HOUR", "CRS_ARR_MIN", "DISTANCE", "DEP_DELAY", "TAXI_OUT", "TAXI_IN", "ARR_DELAY", "ACTUAL_ELAPSED_TIME", "AIR_TIME", "DEP_TIME_HOUR", "DEP_TIME_MIN", "WHEELS_OFF_HOUR", "WHEELS_OFF_MIN", "WHEELS_ON_HOUR", "WHEELS_ON_MIN", "ARR_TIME_HOUR", "ARR_TIME_MIN", "MONTH_SIN", "MONTH_COS", "DAY_SIN", "DAY_COS", "WEEKDAY_SIN", "WEEKDAY_COS"};

    auto function = duckdb::AggregateFunction("triple_sum", {duckdb::LogicalType::ANY}, duckdb::LogicalTypeId::STRUCT, duckdb::AggregateFunction::StateSize<Triple::AggState>,
                             duckdb::AggregateFunction::StateInitialize<Triple::AggState, Triple::StateFunction>, Triple::ListUpdateFunction,
                             Triple::ListCombineFunction, Triple::ListFinalize, nullptr, Triple::ListBindFunction,
                             duckdb::AggregateFunction::StateDestroy<Triple::AggState, Triple::StateFunction>, nullptr, nullptr);

    duckdb::UDFWrapper::RegisterAggrFunction(function, *con.context);
    duckdb::vector<duckdb::LogicalType> args_sum_no_lift = {};
    for(int i=0;i<col_lifted.size();i++)
        args_sum_no_lift.push_back(duckdb::LogicalType::FLOAT);

    auto sum_no_lift = duckdb::AggregateFunction("triple_sum_no_lift", args_sum_no_lift, duckdb::LogicalTypeId::STRUCT, duckdb::AggregateFunction::StateSize<Triple::AggState>,
                                                 duckdb::AggregateFunction::StateInitialize<Triple::AggState, Triple::StateFunction>, Triple::SumNoLift,
                                                 Triple::SumNoLiftCombine, Triple::SumNoLiftFinalize, nullptr, Triple::SumNoLiftBind,
                                                 duckdb::AggregateFunction::StateDestroy<Triple::AggState, Triple::StateFunction>, nullptr, nullptr);
    sum_no_lift.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
    sum_no_lift.varargs = LogicalType::FLOAT;
    duckdb::UDFWrapper::RegisterAggrFunction(sum_no_lift, *con.context, duckdb::LogicalType::FLOAT);

    //define multiply func.

    duckdb::ScalarFunction fun("multiply_triple", {duckdb::LogicalType::ANY}, duckdb::LogicalTypeId::STRUCT, Triple::StructPackFunction, Triple::MultiplyBind, nullptr,
                       Triple::StructPackStats);
    fun.varargs = duckdb::LogicalType::ANY;
    fun.null_handling = duckdb::FunctionNullHandling::SPECIAL_HANDLING;
    fun.serialize = duckdb::VariableReturnBindData::Serialize;
    fun.deserialize = duckdb::VariableReturnBindData::Deserialize;
    duckdb::CreateScalarFunctionInfo info(fun);
    info.schema = DEFAULT_SCHEMA;
    con.context->RegisterFunction(info);


    //custom lift

    duckdb::ScalarFunction custom_lift("lift", {}, duckdb::LogicalTypeId::STRUCT, Triple::CustomLift, Triple::CustomLiftBind, nullptr,
                               Triple::CustomLiftStats);
    custom_lift.varargs = duckdb::LogicalType::FLOAT;
    custom_lift.null_handling = duckdb::FunctionNullHandling::SPECIAL_HANDLING;
    custom_lift.serialize = duckdb::VariableReturnBindData::Serialize;
    custom_lift.deserialize = duckdb::VariableReturnBindData::Deserialize;
    duckdb::CreateScalarFunctionInfo custom_lift_info(custom_lift);
    info.schema = DEFAULT_SCHEMA;
    con.context->RegisterFunction(custom_lift_info);

    //SUM NO LIFT


    std::string lift_query = Triple::lift(col_lifted);

    auto begin = std::chrono::high_resolution_clock::now();
    auto create_table_query = "CREATE TABLE join_table(flight INTEGER,airports INTEGER,OP_CARRIER INTEGER,CRS_DEP_HOUR FLOAT,CRS_DEP_MIN FLOAT,CRS_ARR_HOUR FLOAT,CRS_ARR_MIN FLOAT,ORIGIN INTEGER,DEST INTEGER,DISTANCE FLOAT,index INTEGER,DEP_DELAY FLOAT,TAXI_OUT FLOAT,TAXI_IN FLOAT,ARR_DELAY FLOAT,DIVERTED INTEGER,ACTUAL_ELAPSED_TIME FLOAT,AIR_TIME FLOAT,DEP_TIME_HOUR FLOAT,DEP_TIME_MIN FLOAT,WHEELS_OFF_HOUR FLOAT,WHEELS_OFF_MIN FLOAT,WHEELS_ON_HOUR FLOAT,WHEELS_ON_MIN FLOAT,ARR_TIME_HOUR FLOAT,ARR_TIME_MIN FLOAT,MONTH_SIN FLOAT,MONTH_COS FLOAT,DAY_SIN FLOAT,DAY_COS FLOAT,WEEKDAY_SIN FLOAT,WEEKDAY_COS FLOAT,EXTRA_DAY_ARR INTEGER,EXTRA_DAY_DEP INTEGER);";
    con.Query(create_table_query);
    std::string path = "/Users/massimo/test.csv";
    if (argc > 1)
        path = argv[1];
    con.Query("COPY join_table FROM '"+path+"' (FORMAT CSV, AUTO_DETECT TRUE , nullstr '', DELIMITER ',')");
    auto end = std::chrono::high_resolution_clock::now();
    std::cout<<"Time import data (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

    for (int i=0;i<col_lifted.size();i++){
        int nulls = con.Query("SELECT COUNT(*) FROM join_table WHERE "+col_lifted[i]+" IS NULL")->GetValue<int>(0,0);
        if (nulls > 0) {
            std::cout<<"Removing nulls from "<<col_lifted[i]<<". Nulls: "<<nulls;
            auto query = "UPDATE join_table SET " + col_lifted[i] + " = 0 WHERE " + col_lifted[i] + " IS NULL";
            begin = std::chrono::high_resolution_clock::now();
            con.Query(query);
            end = std::chrono::high_resolution_clock::now();
            std::cout<<". Time (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

            nulls = con.Query("SELECT COUNT(*) FROM join_table WHERE "+col_lifted[i]+" IS NULL")->GetValue<int>(0,0);
            std::cout<<"nulls after: "<<nulls<<"\n";
        }
    }

    begin = std::chrono::high_resolution_clock::now();
    //duckdb::Value v = con.Query("SELECT triple_sum("+lift_query+") from join_table;")->GetValue(0, 0);
    auto test = con.Query("SELECT "+lift_query+" from join_table;")->Fetch();
    end = std::chrono::high_resolution_clock::now();
    std::cout<<"Time lift triples with query (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

    begin = std::chrono::high_resolution_clock::now();
    test = con.Query("SELECT lift(CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN, DISTANCE, DEP_DELAY, TAXI_OUT, TAXI_IN, ARR_DELAY, ACTUAL_ELAPSED_TIME, AIR_TIME, DEP_TIME_HOUR, DEP_TIME_MIN, WHEELS_OFF_HOUR, WHEELS_OFF_MIN, WHEELS_ON_HOUR, WHEELS_ON_MIN, ARR_TIME_HOUR, ARR_TIME_MIN, MONTH_SIN, MONTH_COS, DAY_SIN, DAY_COS, WEEKDAY_SIN, WEEKDAY_COS) from join_table;")->Fetch();
    end = std::chrono::high_resolution_clock::now();
    std::cout<<"Time lift triples with C function (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

    begin = std::chrono::high_resolution_clock::now();
    test = con.Query("SELECT triple_sum(lift(CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN, DISTANCE, DEP_DELAY, TAXI_OUT, TAXI_IN, ARR_DELAY, ACTUAL_ELAPSED_TIME, AIR_TIME, DEP_TIME_HOUR, DEP_TIME_MIN, WHEELS_OFF_HOUR, WHEELS_OFF_MIN, WHEELS_ON_HOUR, WHEELS_ON_MIN, ARR_TIME_HOUR, ARR_TIME_MIN, MONTH_SIN, MONTH_COS, DAY_SIN, DAY_COS, WEEKDAY_SIN, WEEKDAY_COS)) from join_table;")->Fetch();
    end = std::chrono::high_resolution_clock::now();
    std::cout<<"Time SUM after C lift function (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

    begin = std::chrono::high_resolution_clock::now();
    test = con.Query("SELECT triple_sum_no_lift(CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN, DISTANCE, DEP_DELAY, TAXI_OUT, TAXI_IN, ARR_DELAY, ACTUAL_ELAPSED_TIME, AIR_TIME, DEP_TIME_HOUR, DEP_TIME_MIN, WHEELS_OFF_HOUR, WHEELS_OFF_MIN, WHEELS_ON_HOUR, WHEELS_ON_MIN, ARR_TIME_HOUR, ARR_TIME_MIN, MONTH_SIN, MONTH_COS, DAY_SIN, DAY_COS, WEEKDAY_SIN, WEEKDAY_COS) from join_table;")->Fetch();
    end = std::chrono::high_resolution_clock::now();
    std::cout<<"Time SUM without lift (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

    //con.Query("SELECT triple_sum_no_lift(CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN, DISTANCE, DEP_DELAY, TAXI_OUT, TAXI_IN, ARR_DELAY, ACTUAL_ELAPSED_TIME, AIR_TIME, DEP_TIME_HOUR, DEP_TIME_MIN, WHEELS_OFF_HOUR, WHEELS_OFF_MIN, WHEELS_ON_HOUR, WHEELS_ON_MIN, ARR_TIME_HOUR, ARR_TIME_MIN, MONTH_SIN, MONTH_COS, DAY_SIN, DAY_COS, WEEKDAY_SIN, WEEKDAY_COS) from join_table;")->Print();
}
//duckdb::UDFWrapper::RegisterFunction("multiply_triple", {duckdb::LogicalType::STRUCT, duckdb::LogicalTypeId::STRUCT}, duckdb::LogicalTypeId::STRUCT, )
