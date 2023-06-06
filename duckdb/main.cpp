#include "lib/libduckdb/src/duckdb.hpp"
#include "triple/Triple_sum.h"
#include "triple/Triple_mul.h"
#include "triple/Lift.h"
#include "triple/Custom_lift.h"
#include "triple/Sum_no_lift.h"
#include "triple/Triple_sub.h"
#include "ML/Regression.h"

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
    duckdb::DuckDB db(":memory:");
    duckdb::Connection con(db);

    std::vector<string> con_columns = {"CRS_DEP_HOUR", "CRS_DEP_MIN", "CRS_ARR_HOUR", "CRS_ARR_MIN", "DISTANCE", "DEP_DELAY", "TAXI_OUT", "TAXI_IN", "ARR_DELAY", "ACTUAL_ELAPSED_TIME", "AIR_TIME", "DEP_TIME_HOUR", "DEP_TIME_MIN", "WHEELS_OFF_HOUR", "WHEELS_OFF_MIN", "WHEELS_ON_HOUR", "WHEELS_ON_MIN", "ARR_TIME_HOUR", "ARR_TIME_MIN", "MONTH_SIN", "MONTH_COS", "DAY_SIN", "DAY_COS", "WEEKDAY_SIN", "WEEKDAY_COS"};
    std::vector<string> con_columns_nulls = {"WHEELS_ON_HOUR", "WHEELS_OFF_HOUR", "TAXI_OUT", "TAXI_IN", "ARR_DELAY", "DEP_DELAY"};

    std::string table_name = "join_table";

    auto function = duckdb::AggregateFunction("triple_sum", {duckdb::LogicalType::ANY}, duckdb::LogicalTypeId::STRUCT, duckdb::AggregateFunction::StateSize<Triple::AggState>,
                             duckdb::AggregateFunction::StateInitialize<Triple::AggState, Triple::StateFunction>, Triple::ListUpdateFunction,
                             Triple::ListCombineFunction, Triple::ListFinalize, nullptr, Triple::ListBindFunction,
                             duckdb::AggregateFunction::StateDestroy<Triple::AggState, Triple::StateFunction>, nullptr, nullptr);

    duckdb::UDFWrapper::RegisterAggrFunction(function, *con.context);
    duckdb::vector<duckdb::LogicalType> args_sum_no_lift = {};
    for(int i=0; i < con_columns.size(); i++)
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

    //sub triples

    child_list_t<LogicalType> struct_children;
    struct_children.emplace_back("N", LogicalType::INTEGER);
    struct_children.emplace_back("lin_agg", LogicalType::LIST(LogicalType::FLOAT));
    struct_children.emplace_back("quad_agg", LogicalType::LIST(LogicalType::FLOAT));

    auto struct_type = LogicalType::STRUCT(struct_children);

    duckdb::ScalarFunction fun_sub("sub_triple", {struct_type, struct_type}, duckdb::LogicalTypeId::STRUCT, Triple::TripleSubFunction, Triple::TripleSubBind, nullptr,
                               Triple::TripleSubStats);
    fun_sub.varargs = struct_type;
    fun_sub.null_handling = duckdb::FunctionNullHandling::SPECIAL_HANDLING;
    fun_sub.serialize = duckdb::VariableReturnBindData::Serialize;
    fun_sub.deserialize = duckdb::VariableReturnBindData::Deserialize;
    duckdb::CreateScalarFunctionInfo info_sub(fun_sub);
    info_sub.schema = DEFAULT_SCHEMA;
    con.context->RegisterFunction(info_sub);


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

    //
    auto begin = std::chrono::high_resolution_clock::now();
    std::string create_table_query = "CREATE TABLE join_table(flight INTEGER,airports INTEGER,OP_CARRIER INTEGER,CRS_DEP_HOUR FLOAT,CRS_DEP_MIN FLOAT,CRS_ARR_HOUR FLOAT,CRS_ARR_MIN FLOAT,ORIGIN INTEGER,DEST INTEGER,DISTANCE FLOAT,index INTEGER,DEP_DELAY FLOAT,TAXI_OUT FLOAT,TAXI_IN FLOAT,ARR_DELAY FLOAT,DIVERTED INTEGER,ACTUAL_ELAPSED_TIME FLOAT,AIR_TIME FLOAT,DEP_TIME_HOUR FLOAT,DEP_TIME_MIN FLOAT,WHEELS_OFF_HOUR FLOAT,WHEELS_OFF_MIN FLOAT,WHEELS_ON_HOUR FLOAT,WHEELS_ON_MIN FLOAT,ARR_TIME_HOUR FLOAT,ARR_TIME_MIN FLOAT,MONTH_SIN FLOAT,MONTH_COS FLOAT,DAY_SIN FLOAT,DAY_COS FLOAT,WEEKDAY_SIN FLOAT,WEEKDAY_COS FLOAT,EXTRA_DAY_ARR INTEGER,EXTRA_DAY_DEP INTEGER);";
    con.Query(create_table_query);
    std::string path = "/Users/massimo/test.csv";
    if (argc > 1)
        path = argv[1];
    con.Query("COPY join_table FROM '"+path+"' (FORMAT CSV, AUTO_DETECT TRUE , nullstr '', DELIMITER ',')");
    auto end = std::chrono::high_resolution_clock::now();
    std::cout<<"Time import data (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

    con.Query("ALTER TABLE join_table ADD COLUMN n_nulls INTEGER DEFAULT 0;");
    std::string query = "UPDATE join_table SET n_nulls = ";

    for (auto &col : con_columns_nulls) {
        query += "CASE WHEN "+col+" IS NULL THEN 1 ELSE 0 END + ";
    }
    query.pop_back();
    query.pop_back();
    //std::cout<<query<<"\n";
    con.Query(query);

    //START HERE
    //query averages
    query = "SELECT ";
    for (auto &col: con_columns){
        query += "AVG("+col+"), ";
    }
    query.pop_back();
    query.pop_back();
    query += " FROM "+table_name+" LIMIT 10000";
    //std::cout<<query<<"\n";
    //con.Query(query)->Print();
    auto collection = con.Query(query);
    //save averages
    idx_t col_index = 0;
    std::vector<float> avg = {};
    for (auto &col: con_columns){
        Value v = collection->GetValue(col_index, 0);
        avg.push_back(v.GetValue<float>());
        col_index ++;
    }

    //CREATE NEW TABLES
    //0, _col_name, 2
    create_table_query = "CREATE TABLE join_table_complete_0(";

    std::string insert_query = "INSERT INTO join_table_complete_0 SELECT ";

    for (auto &col: con_columns){
        create_table_query += col+" FLOAT, ";
        insert_query += col+", ";
    }
    create_table_query.pop_back();
    create_table_query.pop_back();

    insert_query.pop_back();
    insert_query.pop_back();
    insert_query += " FROM "+table_name+" WHERE n_nulls = 0";
    //std::cout<<create_table_query<<")\n";
    //std::cout<<insert_query<<"\n";
    con.Query(create_table_query+")");
    con.Query(insert_query);

    //create tables 1 missing value
    col_index = 0;
    for(auto &col_null: con_columns_nulls){
        create_table_query = "CREATE TABLE join_table_complete_"+col_null+"(";
        insert_query = "INSERT INTO join_table_complete_"+col_null+" SELECT ";

        for (auto &col: con_columns){
            create_table_query += col+" FLOAT, ";
            if (col == col_null)
                insert_query += "COALESCE ("+col+", "+to_string(avg[col_index])+"), ";
            else
                insert_query += col+", ";
        }
        create_table_query.pop_back();
        create_table_query.pop_back();
        insert_query.pop_back();
        insert_query.pop_back();
        //std::cout<<create_table_query<<")\n";
        //std::cout<<insert_query<<" WHERE n_nulls = 1 AND "<<col_null<<" IS NULL\n";
        con.Query(create_table_query+")");
        con.Query(insert_query+" FROM "+table_name+" WHERE n_nulls = 1 AND "+col_null+" IS NULL");
        col_index ++;
    }

    //create tables >=2 missing values
    create_table_query = "CREATE TABLE join_table_complete_2(";
    insert_query = "INSERT INTO join_table_complete_2 SELECT ";
    col_index = 0;
    for (auto &col: con_columns){
        create_table_query += col+" FLOAT, ";
        auto null = std::find(con_columns_nulls.begin(), con_columns_nulls.end(), col);
        if (null != con_columns_nulls.end()) {
            //this column contains null
            create_table_query += col+"_IS_NULL BOOL, ";
            insert_query += "COALESCE (" + col + ", " + to_string(avg[null - con_columns_nulls.begin()]) + "), "+col+" IS NULL, ";
        }
        else
            insert_query += col+", ";
    }
    create_table_query.pop_back();
    create_table_query.pop_back();
    insert_query.pop_back();
    insert_query.pop_back();

    //std::cout<<create_table_query<<")\n";
    //std::cout<<insert_query<<" WHERE n_nulls >= 2\n";

    con.Query(create_table_query+")");
    con.Query(insert_query+" FROM "+table_name+" WHERE n_nulls >= 2");

    //table is partitioned
    //run MICE

    //compute main cofactor
    query = "SELECT triple_sum_no_lift(";
    for(auto &col: con_columns){
        query +=col+", ";
    }
    query.pop_back();
    query.pop_back();
    query += " ) FROM (SELECT * FROM "+table_name+"_complete_0 ";
    for (auto &col: con_columns_nulls){
        query += " UNION ALL SELECT * FROM "+table_name+"_complete_"+col;
    }
    query += " UNION ALL SELECT ";
    for(auto &col: con_columns){
        query +=col+", ";
    }
    query.pop_back();
    query.pop_back();
    query += " FROM "+table_name+"_complete_2)";

    Value full_triple = con.Query(query)->GetValue(0,0);

    for (int mice_iter =0; mice_iter<1;mice_iter++){
        std::vector<string> testtest = {"WHEELS_ON_HOUR"};
        for(auto &col_null : testtest) {
            //remove nulls
            query = "SELECT triple_sum_no_lift(";
            for (auto &col: con_columns) {
                query += col + ", ";
            }
            query.pop_back();
            query.pop_back();
            query += " ) FROM (SELECT * FROM " + table_name + "_complete_"+col_null+" UNION ALL SELECT ";
            for(auto &col: con_columns){
                query +=col+", ";
            }
            query.pop_back();
            query.pop_back();
            query += " FROM "+table_name+"_complete_2)";

            Value null_triple = con.Query(query)->GetValue(0,0);

            //subtract triples

            query = "SELECT sub_triple("+full_triple.ToString()+", "+null_triple.ToString()+")";
            std::cout<<query<<"\n";
            auto train_cof = con.Query(query)->GetValue(0,0);
            std::cout<<"RES: "<<train_cof.ToString()<<"\n";

            Value train_triple = Triple::subtract_triple(full_triple, null_triple);

            //train

            auto it = std::find(con_columns.begin(), con_columns.end(), col_null);
            int label_index = it - con_columns.begin();

            std::vector <double> params = Triple::ridge_linear_regression(train_triple, label_index, 0.001, 0, 10000);

            //predict query

            std::string new_val = "";
            for(size_t i=0; i< con_columns.size(); i++) {
                if (i==label_index)
                    continue;
                new_val+="("+ to_string(params[i])+" * "+con_columns[i]+")+";
            }
            new_val.pop_back();
            //update 1 missing value
            con.Query("UPDATE "+table_name+"_complete_"+col_null+" SET "+col_null+" = "+new_val)->Print();
            //update 2 missing values
            con.Query("UPDATE "+table_name+"_complete_2 SET "+col_null+" = "+new_val+" WHERE "+col_null+"_IS_NULL")->Print();

            //recompute cofactors

        }
    }



    /*
    //SUM NO LIFT
    std::string lift_query = Triple::lift(con_columns);

    for (int i=0;i<con_columns.size();i++){
        int nulls = con.Query("SELECT COUNT(*) FROM join_table WHERE "+con_columns[i]+" IS NULL")->GetValue<int>(0,0);
        if (nulls > 0) {
            std::cout<<"Removing nulls from "<<con_columns[i]<<". Nulls: "<<nulls;
            auto query = "UPDATE join_table SET " + con_columns[i] + " = 0 WHERE " + con_columns[i] + " IS NULL";
            begin = std::chrono::high_resolution_clock::now();
            con.Query(query);
            end = std::chrono::high_resolution_clock::now();
            std::cout<<". Time (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

            nulls = con.Query("SELECT COUNT(*) FROM join_table WHERE "+con_columns[i]+" IS NULL")->GetValue<int>(0,0);
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
     */
}
//duckdb::UDFWrapper::RegisterFunction("multiply_triple", {duckdb::LogicalType::STRUCT, duckdb::LogicalTypeId::STRUCT}, duckdb::LogicalTypeId::STRUCT, )
