//
// Created by Massimo Perini on 09/06/2023.
//
#include <iostream>
#include <duckdb.hpp>

#include "ML/Regression.h"
#include "triple/helper.h"
#include "partition.h"

#include "triple/From_duckdb.h"
#include "triple/Triple_sub.h"
#include "triple/Triple_sum.h"
#include "ML/Regression_predict.h"
#include <chrono>

#include "ML/lda.h"

#ifdef ENABLE_DOCTEST_IN_LIBRARY
#define DOCTEST_CONFIG_IMPLEMENT
#include "doctest/doctest.h"
#else
#define DOCTEST_CONFIG_DISABLE
#endif


#ifdef _WIN32
#include <Windows.h>
#else
#include <unistd.h>
#endif

int main(int argc, char* argv[]){

    #ifdef ENABLE_DOCTEST_IN_LIBRARY
    doctest::Context ctx;
    ctx.setOption("abort-after", 5);  // default - stop after 5 failed asserts
    ctx.applyCommandLine(argc, argv); // apply command line - argc / argv
    ctx.setOption("no-breaks", true); // override - don't break in the debugger
    int res = ctx.run();              // run test cases unless with --no-run
    if(ctx.shouldExit())              // query flags (and --exit) rely on this
        return res;                   // propagate the result of the tests
    // your code goes here
    #endif
    duckdb::DuckDB db(":memory:");
    duckdb::Connection con(db);

    std::vector<string> con_columns = {"CRS_DEP_HOUR", "CRS_DEP_MIN", "CRS_ARR_HOUR", "CRS_ARR_MIN", "DISTANCE", "DEP_DELAY", "TAXI_OUT", "TAXI_IN", "ARR_DELAY", "ACTUAL_ELAPSED_TIME", "AIR_TIME", "DEP_TIME_HOUR", "DEP_TIME_MIN", "WHEELS_OFF_HOUR", "WHEELS_OFF_MIN", "WHEELS_ON_HOUR", "WHEELS_ON_MIN", "ARR_TIME_HOUR", "ARR_TIME_MIN", "MONTH_SIN", "MONTH_COS", "DAY_SIN", "DAY_COS", "WEEKDAY_SIN", "WEEKDAY_COS"};
    std::vector<string> con_columns_nulls = {"WHEELS_ON_HOUR", "WHEELS_OFF_HOUR", "TAXI_OUT", "TAXI_IN", "ARR_DELAY", "DEP_DELAY"};

    std::vector<string> cat_columns = {};
    std::vector<string> cat_columns_nulls = {};
    std::string table_name = "join_table";
    size_t mice_iters = 1;

    con_columns = {"a","b", "c"};
    cat_columns = {"d","e", "f"};
    Triple::register_functions(*con.context, {con_columns.size()}, {cat_columns.size()});

    std::string ttt = "CREATE TABLE test(gb INTEGER, a FLOAT, b FLOAT, C FLOAT, D INTEGER, E INTEGER, F INTEGER);";
    con.Query(ttt);
    ttt = "INSERT INTO test VALUES (1,1,2,3,4,5,6), (1,5,6,7,8,9,10), (2,2,1,3,4,6,8), (2,5,7,6,8,10,12), (2,2,1,3,4,6,8)";
    con.Query(ttt);
    auto triple = con.Query("SELECT triple_sum_no_lift(a,b,c,d,e,f) FROM test")->GetValue(0,0);
    auto train_params =  lda_train(triple, 0, 0.4);
    train_params.Print();
    std::cout<<"SELECT predict_lda("+train_params.ToString()+"::FLOAT[], a, b,c,e,f) FROM test"<<std::endl;
    con.Query("SELECT predict_lda("+train_params.ToString()+"::FLOAT[], a, b,c,e,f) FROM test")->Print();
    return 0;
    //con.Query("SELECT triple_sum_no_lift(b,c,d,e) FROM test where gb = 1")->Print();
    auto s1 = con.Query("SELECT triple_sum_no_lift(b,c,d,e) FROM test where gb = 1")->GetValue(0,0).ToString();
    con.Query("SELECT lift(a) FROM test where gb = 2")->Print();
    auto s2 = con.Query("SELECT triple_sum_no_lift(a,c,d,f) FROM test where gb = 2")->GetValue(0,0).ToString();
    std::cout<<"SELECT multiply_triple("+s1+", "+s2+")\n";
    con.Query("SELECT triple_sum_no_lift(a,c,d,f) AS B FROM test where gb = 2")->Print();
    std::cout<<"A!\n";
    con.Query("SELECT multiply_triple(A, B) FROM ((SELECT triple_sum_no_lift(b,c,d,e) AS A FROM test where gb = 1) INNER JOIN "
              "(SELECT triple_sum_no_lift(a,c,d,f) AS B FROM test where gb = 2) ON TRUE)")->Print();
    std::cout<<"DONE!\n";


    return 0;

    //import data
    auto begin = std::chrono::high_resolution_clock::now();
    std::string create_table_query = "CREATE TABLE join_table(flight INTEGER,airports INTEGER,OP_CARRIER INTEGER,CRS_DEP_HOUR FLOAT,CRS_DEP_MIN FLOAT,CRS_ARR_HOUR FLOAT,CRS_ARR_MIN FLOAT,ORIGIN INTEGER,DEST INTEGER,DISTANCE FLOAT,index INTEGER,DEP_DELAY FLOAT,TAXI_OUT FLOAT,TAXI_IN FLOAT,ARR_DELAY FLOAT,DIVERTED INTEGER,ACTUAL_ELAPSED_TIME FLOAT,AIR_TIME FLOAT,DEP_TIME_HOUR FLOAT,DEP_TIME_MIN FLOAT,WHEELS_OFF_HOUR FLOAT,WHEELS_OFF_MIN FLOAT,WHEELS_ON_HOUR FLOAT,WHEELS_ON_MIN FLOAT,ARR_TIME_HOUR FLOAT,ARR_TIME_MIN FLOAT,MONTH_SIN FLOAT,MONTH_COS FLOAT,DAY_SIN FLOAT,DAY_COS FLOAT,WEEKDAY_SIN FLOAT,WEEKDAY_COS FLOAT,EXTRA_DAY_ARR INTEGER,EXTRA_DAY_DEP INTEGER);";
    con.Query(create_table_query);
    std::string path = "/Users/massimo/test.csv";
    if (argc > 1)
        path = argv[1];
    con.Query("COPY join_table FROM '"+path+"' (FORMAT CSV, AUTO_DETECT TRUE , nullstr '', DELIMITER ',')");
    auto end = std::chrono::high_resolution_clock::now();
    std::cout<<"Time import data (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

    //compute n. missing values
    con.Query("ALTER TABLE "+table_name+" ADD COLUMN n_nulls INTEGER DEFAULT 0;")->Print();
    std::string query = "CREATE TABLE rep AS SELECT ";

    for (auto &col : con_columns_nulls)
        query += "CASE WHEN "+col+" IS NULL THEN 1 ELSE 0 END + ";
    for (auto &col : cat_columns_nulls)
        query += "CASE WHEN "+col+" IS NULL THEN 1 ELSE 0 END + ";
    query.pop_back();
    query.pop_back();
    con.Query(query+" FROM "+table_name)->Print();
    //swap
    con.Query("ALTER TABLE "+table_name+" ALTER COLUMN n_nulls SET DEFAULT 10;")->Print();//not adding b, replace s with rep

    //partition according to n. missing values
    partition(table_name, con_columns, con_columns_nulls, cat_columns, cat_columns_nulls, con);

    //test


    //--end test



    //run MICE
    //compute main cofactor
    query = "SELECT triple_sum_no_lift(";
    for(auto &col: con_columns){
        query +=col+", ";
    }
    for(auto &col: cat_columns){
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
    for(auto &col: cat_columns){
        query +=col+", ";
    }
    query.pop_back();
    query.pop_back();
    query += " FROM "+table_name+"_complete_2)";

    begin = std::chrono::high_resolution_clock::now();
    con.Query(query)->Print();
    Value full_triple = con.Query(query)->GetValue(0,0);
    end = std::chrono::high_resolution_clock::now();
    std::cout<<"Time full cofactor (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

    //con.Query("SELECT lift(WHEELS_ON_HOUR) FROM join_table")->Print();

    //start MICE

    for (int mice_iter =0; mice_iter<mice_iters;mice_iter++){
        std::vector<string> testtest = {"WHEELS_ON_HOUR"};
        for(auto &col_null : testtest) {
            //remove nulls
            std::string delta_query = "SELECT triple_sum_no_lift(";
            for (auto &col: con_columns)
                delta_query += col + ", ";
            for (auto &col: cat_columns)
                delta_query += col + ", ";

            delta_query.pop_back();
            delta_query.pop_back();
            delta_query += " ) FROM (SELECT * FROM " + table_name + "_complete_"+col_null+" UNION ALL (SELECT ";
            for(auto &col: con_columns)
                delta_query +=col+", ";
            for(auto &col: cat_columns)
                delta_query +=col+", ";
            delta_query.pop_back();
            delta_query.pop_back();
            delta_query += " FROM "+table_name+"_complete_2 WHERE "+col_null+"_IS_NULL))";

            begin = std::chrono::high_resolution_clock::now();
            Value null_triple = con.Query(delta_query)->GetValue(0,0);
            end = std::chrono::high_resolution_clock::now();
            std::cout<<"Time delta cofactor (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

            Value train_triple = Triple::subtract_triple(full_triple, null_triple);
            //train_triple.Print();

            //train
            auto it = std::find(con_columns.begin(), con_columns.end(), col_null);
            size_t label_index = it - con_columns.begin();
            std::cout<<"Label index "<<label_index<<"\n";

            std::vector <double> params = Triple::ridge_linear_regression(train_triple, label_index, 0.001, 0, 1000);
            //std::vector <double> params(test.size(), 0);
            //params[0] = 1;


            //predict query
            std::string new_val = "";
            for(size_t i=0; i< con_columns.size(); i++) {
                if (i==label_index)
                    continue;
                new_val+="("+ to_string((float)params[i])+" * "+con_columns[i]+")+";
            }
            for(size_t i=0; i< cat_columns.size(); i++) {
                new_val+="("+ to_string((float)params[i+con_columns.size()])+" * "+cat_columns[i]+")+";
            }
            new_val.pop_back();
            //update 1 missing value
            std::cout<<"CREATE TABLE rep AS SELECT "+new_val+" AS new_vals FROM "+table_name+"_complete_"+col_null<<std::endl;
            begin = std::chrono::high_resolution_clock::now();
            con.Query("CREATE TABLE rep AS SELECT "+new_val+" AS new_vals FROM "+table_name+"_complete_"+col_null)->Print();
            con.Query("ALTER TABLE "+table_name+"_complete_"+col_null+" ALTER COLUMN "+col_null+" SET DEFAULT 10;")->Print();//not adding b, replace s with rep
            end = std::chrono::high_resolution_clock::now();
            std::cout<<". Time (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

            //update 2 missing values
            //new_val = "10";
            //std::cout<<"UPDATE "+table_name+"_complete_2 SET "+col_null+" = "+new_val+" WHERE "+col_null+"_IS_NULL\n";
            //con.Query("UPDATE "+table_name+"_complete_2 SET "+col_null+" = "+new_val+" WHERE "+col_null+"_IS_NULL")->Print();
            std::cout<<"CREATE TABLE rep AS SELECT CASE WHEN "+col_null+"_IS_NULL THEN "+new_val+" ELSE "+col_null+" END AS test FROM "+table_name+"_complete_2"<<std::endl;
            begin = std::chrono::high_resolution_clock::now();
            con.Query("CREATE TABLE rep AS SELECT CASE WHEN "+col_null+"_IS_NULL THEN "+new_val+" ELSE "+col_null+" END AS test FROM "+table_name+"_complete_2")->Print();
            con.Query("ALTER TABLE "+table_name+"_complete_2 ALTER COLUMN "+col_null+" SET DEFAULT 10;")->Print();//not adding b, replace s with rep
            end = std::chrono::high_resolution_clock::now();
            std::cout<<". Time (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";
            con.Query("SELECT * from "+table_name+"_complete_"+col_null+" LIMIT 100")->Print();
            con.Query("SELECT * from "+table_name+"_complete_2 LIMIT 100")->Print();


            //recompute cofactor
            begin = std::chrono::high_resolution_clock::now();
            null_triple = con.Query(delta_query)->GetValue(0,0);
            end = std::chrono::high_resolution_clock::now();
            std::cout<<"Time delta cofactor (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";
            full_triple = Triple::sum_triple(train_triple, null_triple);

        }
    }
}