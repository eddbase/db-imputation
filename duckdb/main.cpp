
#include <iostream>
#include <duckdb.hpp>

#include "ML/regression.h"
#include "triple/helper.h"

#include "triple/From_duckdb.h"
#include <chrono>
#include "include/flight_partition.h"
#include "include/flight_partition_2.h"
#include "include/flight_baseline.h"

#include "include/train_flight.h"
#include "include/train_retailer.h"
#include "include/column_scalability.h"

#include <boost/version.hpp>
#ifdef ENABLE_DOCTEST_IN_LIBRARY
#define DOCTEST_CONFIG_IMPLEMENT
#include "doctest/doctest.h"
#include "include/factorized_imputation_retailer.h"
#include "include/factorized_imputation_flight.h"

#else
#define DOCTEST_CONFIG_DISABLE
#endif


#ifdef _WIN32
#include <Windows.h>
#else
#include <unistd.h>
#endif

#define TRAIN_TEST

std::string count_n_nulls(const std::vector<std::string> &con_columns_nulls, const std::vector<std::string> &cat_columns_nulls){
    std::string query = "";
    for (auto &col : con_columns_nulls)
        query += "CASE WHEN "+col+" IS NULL THEN 1 ELSE 0 END + ";
    for (auto &col : cat_columns_nulls)
        query += "CASE WHEN "+col+" IS NULL THEN 1 ELSE 0 END + ";
    query.pop_back();
    query.pop_back();
    query+="::INTEGER ";
    return query;
}

std::string count_n_not_nulls(const std::vector<std::string> &con_columns_nulls, const std::vector<std::string> &cat_columns_nulls){
    std::string query = "";
    for (auto &col : con_columns_nulls)
        query += "CASE WHEN "+col+" IS NOT NULL THEN 1 ELSE 0 END + ";
    for (auto &col : cat_columns_nulls)
        query += "CASE WHEN "+col+" IS NOT NULL THEN 1 ELSE 0 END + ";
    query.pop_back();
    query.pop_back();
    query+="::INTEGER ";
    return query;
}

int main(int argc, char* argv[]){

    bool single_table_flight = false;
    bool single_table_retailer = false;
    bool single_table_air_quality = false;
    //bool col_scal_exp = false;
    bool flight_factorized = true;
    bool retailer_factorized = false;
    bool train_flight = false;
    bool train_retailer = false;
    std::string path = "../../make_datasets/flights_dataset";


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
std::cout << "Using Boost "     
          << BOOST_VERSION / 100000     << "."  // major version
          << BOOST_VERSION / 100 % 1000 << "."  // minor version
          << BOOST_VERSION % 100                // patch level
          << std::endl;

    std::string path_train = "../../make_datasets/flight_dataset";//argv[1];
    if(train_retailer) {
        //std::string path_train = "/disk/scratch/imputation_project/systemds/data";//argv[1];
        std::cout << "Start train experiments...\n";
        //Flight::test(path_train);
        //return 0;
        Retailer::train_mat_sql_lift(path, false);
        Retailer::train_mat_sql_lift(path, true);
        Retailer::train_mat_custom_lift(path, false, false);
        Retailer::train_mat_custom_lift(path, false, true);
        Retailer::train_mat_custom_lift(path, true, false);
        Retailer::train_mat_custom_lift(path, true, true);
        Retailer::train_factorized(path, false);
        Retailer::train_factorized(path, true);
    }
    if(train_flight) {
        std::cout << "\n\nTrain\n\n";

        std::cout << "SQL\n";
        Flight::train_mat_sql_lift(path, false);
        std::cout << "SQL mat.\n";
        Flight::train_mat_sql_lift(path, true);
        std::cout << "RING\n";
        Flight::train_mat_custom_lift(path, false, false);
        std::cout << "RING cat\n";
        Flight::train_mat_custom_lift(path, false, true);
        std::cout << "RING mat.\n";
        Flight::train_mat_custom_lift(path, true, false);
        std::cout << "RING mat. cat.\n";
        Flight::train_mat_custom_lift(path, true, true);
        std::cout << "RING ring + fact.\n";
        Flight::train_factorized(path, false);
        std::cout << "RING ring + fact. cat.\n";
        Flight::train_factorized(path, true);

    }

    if (single_table_flight) {

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
    std::string create_table_query = "CREATE TABLE join_table(flight INTEGER,airports INTEGER,OP_CARRIER INTEGER,CRS_DEP_HOUR FLOAT,CRS_DEP_MIN FLOAT,CRS_ARR_HOUR FLOAT,CRS_ARR_MIN FLOAT,ORIGIN INTEGER,DEST INTEGER,DISTANCE FLOAT,index INTEGER,DEP_DELAY FLOAT,TAXI_OUT FLOAT,TAXI_IN FLOAT,ARR_DELAY FLOAT,DIVERTED INTEGER,ACTUAL_ELAPSED_TIME FLOAT,AIR_TIME FLOAT,DEP_TIME_HOUR FLOAT,DEP_TIME_MIN FLOAT,WHEELS_OFF_HOUR FLOAT,WHEELS_OFF_MIN FLOAT,WHEELS_ON_HOUR FLOAT,WHEELS_ON_MIN FLOAT,ARR_TIME_HOUR FLOAT,ARR_TIME_MIN FLOAT,MONTH_SIN FLOAT,MONTH_COS FLOAT,DAY_SIN FLOAT,DAY_COS FLOAT,WEEKDAY_SIN FLOAT,WEEKDAY_COS FLOAT,EXTRA_DAY_ARR INTEGER,EXTRA_DAY_DEP INTEGER);";
    con.Query(create_table_query);
    if (argc > 1)
        path = argv[1];
    std::cout<<path<<"/join_table_flights.csv";

    con.Query("SET preserve_insertion_order = true");
    int parallelism = con.Query("SELECT current_setting('threads')")->GetValue<int>(0, 0);
    std::cout<<"current parallelism : "<<parallelism<<std::endl;

    con.Query("COPY join_table FROM '"+path+"/join_table_flights.csv' (FORMAT CSV, AUTO_DETECT TRUE , nullstr '', DELIMITER ',')");
    con.Query("CREATE TABLE join_table_tmp AS SELECT flight,airports,OP_CARRIER,CRS_DEP_HOUR,CRS_DEP_MIN,CRS_ARR_HOUR,"
              "CRS_ARR_MIN,ORIGIN,DEST,DISTANCE,index,DEP_DELAY,TAXI_OUT,TAXI_IN,ARR_DELAY,DIVERTED,ACTUAL_ELAPSED_TIME,"
              "AIR_TIME,DEP_TIME_HOUR,DEP_TIME_MIN,WHEELS_OFF_HOUR,WHEELS_OFF_MIN,WHEELS_ON_HOUR,WHEELS_ON_MIN,ARR_TIME_HOUR"
              ",ARR_TIME_MIN,MONTH_SIN,MONTH_COS,DAY_SIN,DAY_COS,WEEKDAY_SIN,WEEKDAY_COS,EXTRA_DAY_ARR,EXTRA_DAY_DEP,"+
              count_n_nulls(con_columns_nulls, cat_columns_nulls)+" AS n_nulls, "+ count_n_not_nulls(con_columns_nulls, cat_columns_nulls)+
              " AS n_not_nulls FROM join_table ORDER BY flight");
    con.Query("DROP TABLE join_table");
    con.Query("ALTER TABLE join_table_tmp RENAME TO join_table");

    std::cout<<"---- Partitioned ----\n";
    run_flight_partition(con, con_columns, cat_columns, con_columns_nulls, cat_columns_nulls, table_name, mice_iters, "index");
    run_flight_partition_alt(con, con_columns, cat_columns, con_columns_nulls, cat_columns_nulls, table_name, mice_iters, "index");
    std::cout<<"---- Baseline ----\n";
    run_flight_baseline(con, con_columns, cat_columns, con_columns_nulls, cat_columns_nulls, table_name, mice_iters, "index");
    }
    if (single_table_retailer) {
duckdb::DuckDB db(":memory:");
        duckdb::Connection con(db);
        std::vector<std::string> con_columns = {"population", "white", "asian", "pacific", "black", "medianage", "occupiedhouseunits",
                                                "houseunits", "families", "households", "husbwife", "males", "females", "householdschildren",
                                                "hispanic", "rgn_cd", "clim_zn_nbr", "tot_area_sq_ft", "sell_area_sq_ft", "avghhi", "supertargetdistance",
                                                "supertargetdrivetime", "targetdistance", "targetdrivetime", "walmartdistance", "walmartdrivetime",
                                                "walmartsupercenterdistance", "walmartsupercenterdrivetime" , "prize", "feat_1", "maxtemp", "mintemp",
                                                "meanwind", "inventoryunits"};
        std::vector<std::string> con_columns_nulls = {"inventoryunits", "maxtemp", "mintemp", "supertargetdistance", "walmartdistance"};

        std::vector<std::string> cat_columns = {"subcategory", "category", "categoryCluster", "rain", "snow", "thunder"};
        std::vector<std::string> cat_columns_nulls = {"rain", "snow"};
        std::string table_name = "join_table";
        size_t mice_iters = 1;

        Triple::register_functions(*con.context, {con_columns.size()}, {cat_columns.size()});

        //con.Query("SELECT triple_sum_no_lift(b,c,d,e) FROM test where gb = 1")->Print();
        //import data
        auto begin = std::chrono::high_resolution_clock::now();
        std::string create_table_query = "CREATE TABLE join_table("
                                         "    locn INTEGER,"
                                         "    dateid INTEGER,"
                                         "    rain INTEGER,"
                                         "    snow INTEGER,"
                                         "    maxtemp FLOAT,"
                                         "    mintemp FLOAT,"
                                         "    meanwind FLOAT,"
                                         "    thunder INTEGER,"
                                         "    zip INTEGER,"
                                         "    rgn_cd FLOAT,"
                                         "    clim_zn_nbr FLOAT,"
                                         "    tot_area_sq_ft FLOAT,"
                                         "    sell_area_sq_ft FLOAT,"
                                         "    avghhi FLOAT,"
                                         "    supertargetdistance FLOAT,"
                                         "    supertargetdrivetime FLOAT,"
                                         "    targetdistance FLOAT,"
                                         "    targetdrivetime FLOAT,"
                                         "    walmartdistance FLOAT,"
                                         "    walmartdrivetime FLOAT,"
                                         "    walmartsupercenterdistance FLOAT,"
                                         "    walmartsupercenterdrivetime FLOAT,"
                                         "    ksn INTEGER,"
                                         "    inventoryunits FLOAT,"
                                         "    subcategory INTEGER, "
                                         "    category INTEGER, "
                                         "    categoryCluster INTEGER,"
                                         "    prize FLOAT,"
                                         "    feat_1 FLOAT,"
                                         "    population FLOAT,"
                                         "    white FLOAT,"
                                         "    asian FLOAT,"
                                         "    pacific FLOAT,"
                                         "    black FLOAT,"
                                         "    medianage FLOAT,"
                                         "    occupiedhouseunits FLOAT,"
                                         "    houseunits FLOAT,"
                                         "    families FLOAT,"
                                         "    households FLOAT,"
                                         "    husbwife FLOAT,"
                                         "    males FLOAT,"
                                         "    females FLOAT,"
                                         "    householdschildren FLOAT,"
                                         "    hispanic FLOAT"
                                         ");";
        con.Query(create_table_query);
        std::string path = "/disk/scratch/imputation_project/systemds/data/join_table_retailer_20.csv";
        con.Query("SET preserve_insertion_order = true");
        int parallelism = con.Query("SELECT current_setting('threads')")->GetValue<int>(0, 0);
        std::cout<<"current parallelism : "<<parallelism<<std::endl;
        //con.Query("SET threads TO 1;")
        if (argc > 1)
            path = argv[1];
        con.Query("COPY join_table FROM '" + path + "' (FORMAT CSV, AUTO_DETECT TRUE , nullstr '', DELIMITER ',')");
        auto end = std::chrono::high_resolution_clock::now();
        std::cout << "Time import data (ms): "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << "\n";
        std::cout << "---- Flight Partitioned ----\n";
        run_flight_partition(con, con_columns, cat_columns, con_columns_nulls, cat_columns_nulls, table_name,
                             mice_iters, "flight");
        std::cout << "---- Baseline ----\n";
        run_flight_baseline(con, con_columns, cat_columns, con_columns_nulls, cat_columns_nulls, table_name,
                            mice_iters, "flight");
    }


if (single_table_air_quality) {
        duckdb::DuckDB db(":memory:");
        duckdb::Connection con(db);

        std::vector<std::string> con_columns = {"AQI", "CO", "O3", "PM10", "PM2_5", "NO2", "NOx", "NO_", "WindSpeed", "WindDirec", "SO2_AVG"};

        std::vector<std::string> cat_columns = {};
        std::vector<std::string> con_columns_nulls = {"CO", "O3", "PM10", "PM2_5", "NO2", "NOx", "NO_", "WindSpeed", "WindDirec", "SO2_AVG"};
        std::vector<std::string> cat_columns_nulls = {};
        std::string table_name = "join_table";
        size_t mice_iters = 1;

        Triple::register_functions(*con.context, {con_columns.size()}, {cat_columns.size()});

        //con.Query("SELECT triple_sum_no_lift(b,c,d,e) FROM test where gb = 1")->Print();
        //import data
        auto begin = std::chrono::high_resolution_clock::now();
        std::string create_table_query = "CREATE TABLE join_table("
                                         "    AQI FLOAT,"
                                         "    CO FLOAT,"
                                         "    O3 FLOAT,"
                                         "    PM10 FLOAT,"
                                         "    PM2_5 FLOAT,"
                                         "    NO2 FLOAT,"
                                         "    NOx FLOAT,"
                                         "    NO_ FLOAT,"
                                         "    WindSpeed FLOAT,"
                                         "    WindDirec FLOAT,"
                                         "    SO2_AVG FLOAT"
                                         ");";
        con.Query(create_table_query);
        if (argc > 1)
            path = argv[1];
        std::cout<<path<<"/air_quality_postgres.csv";
        con.Query("COPY join_table FROM '" + path + "/air_quality_postgres.csv' (FORMAT CSV, AUTO_DETECT TRUE , nullstr '', DELIMITER ',')");
        auto end = std::chrono::high_resolution_clock::now();
        std::cout << "Time import data (ms): "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << "\n";
        std::cout << "---- Flight Partitioned ----\n";
        run_flight_partition(con, con_columns, cat_columns, con_columns_nulls, cat_columns_nulls, table_name,
                             mice_iters, "");
        std::cout << "---- Baseline ----\n";
        run_flight_baseline(con, con_columns, cat_columns, con_columns_nulls, cat_columns_nulls, table_name,
                            mice_iters, "");
    }
/*
if(col_scal_exp){
            duckdb::DuckDB db(":memory:");
            duckdb::Connection con(db);

            std::vector<std::string> con_columns = {"CRS_DEP_HOUR", "CRS_DEP_MIN", "CRS_ARR_HOUR", "CRS_ARR_MIN",
                                                    "DISTANCE", "DEP_DELAY", "TAXI_OUT", "TAXI_IN", "ARR_DELAY",
                                                    "ACTUAL_ELAPSED_TIME", "AIR_TIME", "DEP_TIME_HOUR", "DEP_TIME_MIN",
                                                    "WHEELS_OFF_HOUR", "WHEELS_OFF_MIN", "WHEELS_ON_HOUR", "WHEELS_ON_MIN",
                                                    "ARR_TIME_HOUR", "ARR_TIME_MIN", "MONTH_SIN", "MONTH_COS", "DAY_SIN",
                                                    "DAY_COS", "WEEKDAY_SIN", "WEEKDAY_COS"};
            std::vector<std::string> con_columns_nulls = {"WHEELS_ON_HOUR", "WHEELS_OFF_HOUR", "TAXI_OUT", "TAXI_IN",
                                                          "ARR_DELAY", "DEP_DELAY"};

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
            std::string path = "/disk/scratch/imputation_project/systemds/data/join_table_flights_0.05.csv";
            if (argc > 1)
                path = argv[1];
            con.Query("COPY join_table FROM '" + path + "' (FORMAT CSV, AUTO_DETECT TRUE , nullstr '', DELIMITER ',')");
            auto end = std::chrono::high_resolution_clock::now();
            std::cout << "Time import data (ms): "
                      << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << "\n";
            std::cout << "---- Flight Partitioned ----\n";
            int col_nulls[4] = {1,2,4,6};
            for(auto exp_col_null : col_nulls) {
                std::cout<<"NUM COL NULLS: "<<exp_col_null<<"\n";
                std::vector<std::string> assume_null(con_columns_nulls.begin(), con_columns_nulls.begin()+exp_col_null);
                scalability_col_exp(con, con_columns, cat_columns, con_columns_nulls, cat_columns_nulls, table_name,
                                    mice_iters, assume_null);

                con.Query("DROP TABLE join_table_complete_0");
                con.Query("DROP TABLE join_table_complete_2");
                for(auto col: assume_null)
                    con.Query("DROP TABLE join_table_complete_"+col);
                con.Query("ALTER TABLE join_table DROP n_nulls;");

            }
    }
*/
    if (flight_factorized) {
        run_flight_partition_factorized_flight(path, "join_table", 1);
    }

    if (retailer_factorized) {
        run_flight_partition_factorized_retailer(path, "join_table", 1);
    }


return 0;
}
