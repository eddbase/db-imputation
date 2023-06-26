//
// Created by Massimo Perini on 09/06/2023.
//
#include <iostream>
#include <duckdb.hpp>

#include "ML/Regression.h"
#include "triple/helper.h"

#include "triple/From_duckdb.h"
#include <chrono>
#include "experiments/flight_partition.h"
#include "experiments/flight_baseline.h"


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
    /*doctest::Context ctx;
    ctx.setOption("abort-after", 5);  // default - stop after 5 failed asserts
    ctx.applyCommandLine(argc, argv); // apply command line - argc / argv
    ctx.setOption("no-breaks", true); // override - don't break in the debugger
    int res = ctx.run();              // run test cases unless with --no-run
    if(ctx.shouldExit())              // query flags (and --exit) rely on this
        return res;                   // propagate the result of the tests
    // your code goes here*/
    #endif
    duckdb::DuckDB db(":memory:");
    duckdb::Connection con(db);

    std::vector<std::string> con_columns = {"CRS_DEP_HOUR", "CRS_DEP_MIN", "CRS_ARR_HOUR", "CRS_ARR_MIN", "DISTANCE", "DEP_DELAY", "TAXI_OUT", "TAXI_IN", "ARR_DELAY", "ACTUAL_ELAPSED_TIME", "AIR_TIME", "DEP_TIME_HOUR", "DEP_TIME_MIN", "WHEELS_OFF_HOUR", "WHEELS_OFF_MIN", "WHEELS_ON_HOUR", "WHEELS_ON_MIN", "ARR_TIME_HOUR", "ARR_TIME_MIN", "MONTH_SIN", "MONTH_COS", "DAY_SIN", "DAY_COS", "WEEKDAY_SIN", "WEEKDAY_COS"};
    std::vector<std::string> con_columns_nulls = {"WHEELS_ON_HOUR", "WHEELS_OFF_HOUR", "TAXI_OUT", "TAXI_IN", "ARR_DELAY", "DEP_DELAY"};

    std::vector<std::string> cat_columns = {"OP_CARRIER", "DIVERTED", "EXTRA_DAY_DEP", "EXTRA_DAY_ARR"};
    std::vector<std::string> cat_columns_nulls = {"DIVERTED"};
    std::string table_name = "join_table";
    size_t mice_iters = 1;

    Triple::register_functions(*con.context, {con_columns.size()}, {cat_columns.size()});


    //con.Query("SELECT triple_sum_no_lift(b,c,d,e) FROM test where gb = 1")->Print();


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
    std::cout<<"---- Partitioned ----\n";
    run_flight_partition(con, con_columns, cat_columns, con_columns_nulls, cat_columns_nulls, table_name, mice_iters);
    std::cout<<"---- Baseline ----\n";
    run_flight_baseline(con, con_columns, cat_columns, con_columns_nulls, cat_columns_nulls, table_name, mice_iters);
}