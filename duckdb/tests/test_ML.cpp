#include <doctest/doctest.h>
#include <duckdb.hpp>
#include <triple/helper.h>

#include <iostream>

#include <vector>

#include <factorized_imputation_flight.h>
#include <iostream>
#include <partition.h>
#include <triple/sub.h>
#include <triple/sum/sum.h>
#include <ML/lda.h>
#include <ML/regression.h>
#include <triple/helper.h>

using namespace std;

static void import_data(duckdb::Connection &con, const std::string &path) {
    con.Query("CREATE TABLE route (ROUTE_ID INTEGER PRIMARY KEY, ORIGIN INT, DEST INT, DISTANCE FLOAT);");
    con.Query(
            "CREATE TABLE schedule (SCHEDULE_ID INTEGER PRIMARY KEY, OP_CARRIER INT, CRS_DEP_HOUR FLOAT, CRS_DEP_MIN FLOAT, CRS_ARR_HOUR FLOAT, CRS_ARR_MIN FLOAT, ROUTE_ID INTEGER);");
    con.Query(
            "CREATE TABLE join_table (id INTEGER PRIMARY KEY, DEP_DELAY FLOAT, TAXI_OUT FLOAT, TAXI_IN FLOAT, ARR_DELAY FLOAT, DIVERTED INTEGER, ACTUAL_ELAPSED_TIME FLOAT, AIR_TIME FLOAT, DEP_TIME_HOUR FLOAT, DEP_TIME_MIN FLOAT, "
            "WHEELS_OFF_HOUR FLOAT, WHEELS_OFF_MIN FLOAT, WHEELS_ON_HOUR FLOAT, WHEELS_ON_MIN FLOAT, ARR_TIME_HOUR FLOAT, ARR_TIME_MIN FLOAT, MONTH_SIN FLOAT, MONTH_COS FLOAT, DAY_SIN FLOAT, DAY_COS FLOAT, WEEKDAY_SIN "
            "FLOAT, WEEKDAY_COS FLOAT, EXTRA_DAY_ARR INTEGER, EXTRA_DAY_DEP INTEGER, SCHEDULE_ID INTEGER);");

    con.Query("COPY route FROM '" + path +
              "/route_dataset.csv' (FORMAT CSV, AUTO_DETECT TRUE , nullstr '', DELIMITER ',')");
    con.Query("COPY schedule FROM '" + path +
              "/schedule_dataset.csv' (FORMAT CSV, AUTO_DETECT TRUE , nullstr '', DELIMITER ',')");
    con.Query("COPY join_table FROM '" + path +
              "/airlines_dataset.csv' (FORMAT CSV, AUTO_DETECT TRUE , nullstr '', DELIMITER ',')");
}

TEST_CASE("ML") {
    duckdb::DuckDB db(":memory:");
    duckdb::Connection con(db);
    //import data
    std::string path = "../../make_datasets/flights_dataset";
    std::string fact_table_name = "join_table";

    import_data(con, path);
    //con.Query("SELECT * FROM join_table limit 5")->Print();
    //con.Query("SELECT * FROM schedule limit 5")->Print();
    //con.Query("SELECT * FROM route limit 5")->Print();


    std::vector<std::string> con_columns_fact = {"DEP_DELAY", "TAXI_OUT", "TAXI_IN", "ARR_DELAY",
                                                 "ACTUAL_ELAPSED_TIME", "AIR_TIME", "DEP_TIME_HOUR", "DEP_TIME_MIN",
                                                 "WHEELS_OFF_HOUR", "WHEELS_OFF_MIN", "WHEELS_ON_HOUR", "WHEELS_ON_MIN",
                                                 "ARR_TIME_HOUR", "ARR_TIME_MIN", "MONTH_SIN", "MONTH_COS", "DAY_SIN",
                                                 "DAY_COS", "WEEKDAY_SIN", "WEEKDAY_COS"};
    std::vector<std::string> cat_columns_fact = {"DIVERTED", "EXTRA_DAY_ARR", "EXTRA_DAY_DEP"};

    std::vector<std::string> cat_columns_fact_null = {"DIVERTED"};
    std::vector<std::string> con_columns_fact_null = {"DEP_DELAY", "TAXI_OUT", "TAXI_IN", "ARR_DELAY", "WHEELS_OFF_HOUR", "WHEELS_ON_HOUR"};

    std::vector<std::string> con_columns_join = {"DEP_DELAY", "TAXI_OUT", "TAXI_IN", "ARR_DELAY",
                                                 "ACTUAL_ELAPSED_TIME", "AIR_TIME", "DEP_TIME_HOUR", "DEP_TIME_MIN",
                                                 "WHEELS_OFF_HOUR", "WHEELS_OFF_MIN", "WHEELS_ON_HOUR", "WHEELS_ON_MIN",
                                                 "ARR_TIME_HOUR", "ARR_TIME_MIN", "MONTH_SIN", "MONTH_COS", "DAY_SIN",
                                                 "DAY_COS", "WEEKDAY_SIN", "WEEKDAY_COS", "CRS_DEP_HOUR", "CRS_DEP_MIN", "CRS_ARR_HOUR", "CRS_ARR_MIN"};

    std::vector<std::string> cat_columns_join = {"DIVERTED", "EXTRA_DAY_ARR", "EXTRA_DAY_DEP", "OP_CARRIER"};
    Triple::register_functions(*con.context, {20, 24}, {3, 4});//con+cat, con only

    con.Query("ALTER TABLE "+fact_table_name+" ADD COLUMN n_nulls INTEGER DEFAULT 10;")->Print();
    std::string query = "CREATE TABLE rep AS SELECT ";

    for (auto &col: con_columns_fact_null)
        query += "CASE WHEN " + col + " IS NULL THEN 1 ELSE 0 END + ";
    for (auto &col: cat_columns_fact_null)
        query += "CASE WHEN " + col + " IS NULL THEN 1 ELSE 0 END + ";
    query.pop_back();
    query.pop_back();
    con.Query(query + "::INTEGER FROM " + fact_table_name);
    //swap
    //works only with another vector (flat vector)
    con.Query("ALTER TABLE " + fact_table_name +
              " ALTER COLUMN n_nulls SET DEFAULT 10;")->Print();//not adding b, replacing column

    con.Query("SET threads TO 1;");
    std::vector<std::pair<std::string, std::string>> cat_col_info;
    cat_col_info.push_back(std::make_pair("DIVERTED", "join_table"));
    cat_col_info.push_back(std::make_pair("EXTRA_DAY_ARR", "join_table"));
    cat_col_info.push_back(std::make_pair("EXTRA_DAY_DEP", "join_table"));
    cat_col_info.push_back(std::make_pair("OP_CARRIER", "schedule"));
    build_list_of_uniq_categoricals(cat_col_info, con);

    con.Query("CREATE TABLE join_table_2_exp AS SELECT * FROM schedule JOIN join_table USING (SCHEDULE_ID)");

    //partition according to n. missing values
    auto begin = std::chrono::high_resolution_clock::now();
    std::vector<std::string> vals_partition = std::vector(cat_columns_fact);
    vals_partition.push_back("id");
    vals_partition.push_back("SCHEDULE_ID");//20, 19
    partition(fact_table_name, con_columns_fact, con_columns_fact_null, vals_partition, cat_columns_fact_null, con, "id");//
    auto end = std::chrono::high_resolution_clock::now();
    std::cout<<"Time partitioning fact table (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

    //run MICE
    //compute main cofactor
    std::string col_select = "";
    for(auto &col: con_columns_fact){
        col_select +=col+", ";
    }
    for(auto &col: cat_columns_fact){
        col_select +=col+", ";
    }
    col_select+="SCHEDULE_ID";
    //triple_sum(multiply_triple(cnt2
    query = "SELECT triple_sum(multiply_triple(cnt2, cnt1)) FROM "
            "(SELECT SCHEDULE_ID, triple_sum_no_lift1(DEP_DELAY, TAXI_OUT, TAXI_IN, ARR_DELAY, ACTUAL_ELAPSED_TIME, AIR_TIME, DEP_TIME_HOUR, DEP_TIME_MIN, WHEELS_OFF_HOUR, WHEELS_OFF_MIN, WHEELS_ON_HOUR, WHEELS_ON_MIN,  ARR_TIME_HOUR, ARR_TIME_MIN, MONTH_SIN, MONTH_COS, DAY_SIN, DAY_COS, WEEKDAY_SIN, WEEKDAY_COS, DIVERTED, EXTRA_DAY_ARR, EXTRA_DAY_DEP) AS cnt2 "
            " FROM "
            "( SELECT "+col_select+" FROM "+fact_table_name+"_complete_0 ";
    for (auto &con_col : con_columns_fact_null)
        query += " UNION ALL SELECT "+col_select+" FROM "+fact_table_name+"_complete_"+con_col;
    for (auto &cat_col : cat_columns_fact_null)
        query += " UNION ALL SELECT "+col_select+" FROM "+fact_table_name+"_complete_"+cat_col;
    query += " UNION ALL SELECT "+col_select+" FROM "+fact_table_name+"_complete_2) ";
    query += "as flight group by SCHEDULE_ID) as flight JOIN ("
             "    SELECT SCHEDULE_ID, lift(CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN, OP_CARRIER) as cnt1"
             "    FROM schedule) as schedule ON flight.SCHEDULE_ID = schedule.SCHEDULE_ID;";

    std::cout<<query<<"\n";
    std::cout<<con.Query(query)->ToString()<<std::endl;
    con.Query("SET threads TO 1;");
    //std::cout<<con.Query(query)->ToString()<<std::endl;
    begin = std::chrono::high_resolution_clock::now();
    duckdb::Value full_triple = con.Query(query)->GetValue(0,0);
    end = std::chrono::high_resolution_clock::now();
    std::cout<<"Time full cofactor (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";
    std::cout<<full_triple.ToString()<<std::endl;
    full_triple.Print();



    con_columns_fact = {"DEP_DELAY", "TAXI_OUT", "TAXI_IN", "ARR_DELAY",
                        "ACTUAL_ELAPSED_TIME", "AIR_TIME", "DEP_TIME_HOUR", "DEP_TIME_MIN",
                        "WHEELS_OFF_HOUR", "WHEELS_OFF_MIN", "WHEELS_ON_HOUR", "WHEELS_ON_MIN",
                        "ARR_TIME_HOUR", "ARR_TIME_MIN", "MONTH_SIN", "MONTH_COS", "DAY_SIN",
                        "DAY_COS", "WEEKDAY_SIN", "WEEKDAY_COS", "CRS_DEP_HOUR", "CRS_DEP_MIN", "CRS_ARR_HOUR", "CRS_ARR_MIN"};

    vals_partition = std::vector(cat_columns_fact);
    vals_partition.push_back("DIVERTED");
    vals_partition.push_back("OP_CARRIER");
    vals_partition.push_back("id");
    vals_partition.push_back("SCHEDULE_ID");//20, 19
//triple_sum_no_lift2
    std::cout<<con.Query("SELECT COUNT(*) FROM join_table_2_exp WHERE OP_CARRIER == 15")->ToString()<<std::endl;

    //distinct op_carrier
    query = "SELECT DISTINCT OP_CARRIER FROM schedule WHERE SCHEDULE_ID IN (SELECT SCHEDULE_ID FROM join_table_complete_0 ";
    for (auto &con_col : con_columns_fact_null)
        query += " UNION ALL SELECT SCHEDULE_ID FROM join_table_complete_"+con_col;
    for (auto &cat_col : cat_columns_fact_null)
        query += " UNION ALL SELECT SCHEDULE_ID FROM join_table_complete_"+cat_col;
    query += " UNION ALL SELECT SCHEDULE_ID FROM join_table_complete_2) ";

    std::cout<<con.Query(query)->ToString()<<std::endl;


    query = "SELECT flight.SCHEDULE_ID, triple FROM "
            "(SELECT SCHEDULE_ID AS SCHEDULE_ID, SUM(DEP_DELAY) AS DEP_DELAY"
            " FROM "
            "( SELECT SCHEDULE_ID, (DEP_DELAY) AS DEP_DELAY FROM "+fact_table_name+"_complete_0 ";
    for (auto &con_col : con_columns_fact_null)
        query += " UNION ALL SELECT SCHEDULE_ID, (DEP_DELAY) AS DEP_DELAY FROM "+fact_table_name+"_complete_"+con_col;
    for (auto &cat_col : cat_columns_fact_null)
        query += " UNION ALL SELECT SCHEDULE_ID, (DEP_DELAY) AS DEP_DELAY FROM "+fact_table_name+"_complete_"+cat_col;
    query += " UNION ALL SELECT SCHEDULE_ID, (DEP_DELAY) AS DEP_DELAY FROM "+fact_table_name+"_complete_2) ";
    query += "as flight group by SCHEDULE_ID) as flight JOIN ("
             "    SELECT SCHEDULE_ID AS SCHEDULE_ID, lift(CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN, OP_CARRIER) as triple"
             "    FROM schedule) as schedule ON flight.SCHEDULE_ID = schedule.SCHEDULE_ID;";

    //std::cout<<con.Query(query)->ToString()<<std::endl;

    partition("join_table_2_exp", con_columns_fact, con_columns_fact_null, vals_partition, cat_columns_fact_null, con, "id");//
    query = "SELECT triple_sum(lift(DEP_DELAY, TAXI_OUT, TAXI_IN, ARR_DELAY, ACTUAL_ELAPSED_TIME, AIR_TIME, DEP_TIME_HOUR, DEP_TIME_MIN, WHEELS_OFF_HOUR, WHEELS_OFF_MIN, WHEELS_ON_HOUR, WHEELS_ON_MIN,  ARR_TIME_HOUR, ARR_TIME_MIN, MONTH_SIN, MONTH_COS, DAY_SIN, DAY_COS, WEEKDAY_SIN, WEEKDAY_COS, CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN, DIVERTED, EXTRA_DAY_ARR, EXTRA_DAY_DEP, OP_CARRIER)) FROM "
            "( SELECT DEP_DELAY, TAXI_OUT, TAXI_IN, ARR_DELAY, ACTUAL_ELAPSED_TIME, AIR_TIME, DEP_TIME_HOUR, DEP_TIME_MIN, WHEELS_OFF_HOUR, WHEELS_OFF_MIN, WHEELS_ON_HOUR, WHEELS_ON_MIN,  ARR_TIME_HOUR, ARR_TIME_MIN, MONTH_SIN, MONTH_COS, DAY_SIN, DAY_COS, WEEKDAY_SIN, WEEKDAY_COS, CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN, DIVERTED, EXTRA_DAY_ARR, EXTRA_DAY_DEP, OP_CARRIER FROM join_table_2_exp_complete_0 ";

    for (auto &con_col : con_columns_fact_null)
        query += " UNION ALL SELECT DEP_DELAY, TAXI_OUT, TAXI_IN, ARR_DELAY, ACTUAL_ELAPSED_TIME, AIR_TIME, DEP_TIME_HOUR, DEP_TIME_MIN, WHEELS_OFF_HOUR, WHEELS_OFF_MIN, WHEELS_ON_HOUR, WHEELS_ON_MIN,  ARR_TIME_HOUR, ARR_TIME_MIN, MONTH_SIN, MONTH_COS, DAY_SIN, DAY_COS, WEEKDAY_SIN, WEEKDAY_COS, CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN, DIVERTED, EXTRA_DAY_ARR, EXTRA_DAY_DEP, OP_CARRIER FROM join_table_2_exp_complete_"+con_col;
    for (auto &cat_col : cat_columns_fact_null)
        query += " UNION ALL SELECT DEP_DELAY, TAXI_OUT, TAXI_IN, ARR_DELAY, ACTUAL_ELAPSED_TIME, AIR_TIME, DEP_TIME_HOUR, DEP_TIME_MIN, WHEELS_OFF_HOUR, WHEELS_OFF_MIN, WHEELS_ON_HOUR, WHEELS_ON_MIN,  ARR_TIME_HOUR, ARR_TIME_MIN, MONTH_SIN, MONTH_COS, DAY_SIN, DAY_COS, WEEKDAY_SIN, WEEKDAY_COS, CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN, DIVERTED, EXTRA_DAY_ARR, EXTRA_DAY_DEP, OP_CARRIER FROM join_table_2_exp_complete_"+cat_col;
    query += " UNION ALL SELECT  DEP_DELAY, TAXI_OUT, TAXI_IN, ARR_DELAY, ACTUAL_ELAPSED_TIME, AIR_TIME, DEP_TIME_HOUR, DEP_TIME_MIN, WHEELS_OFF_HOUR, WHEELS_OFF_MIN, WHEELS_ON_HOUR, WHEELS_ON_MIN,  ARR_TIME_HOUR, ARR_TIME_MIN, MONTH_SIN, MONTH_COS, DAY_SIN, DAY_COS, WEEKDAY_SIN, WEEKDAY_COS, CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN, DIVERTED, EXTRA_DAY_ARR, EXTRA_DAY_DEP, OP_CARRIER FROM join_table_2_exp_complete_2)";

    //std::cout<<con.Query(query)->ToString()<<std::endl;

    duckdb::Value full_triple2 = con.Query(query)->GetValue(0,0);

    std::cout<<full_triple2.ToString()<<std::endl;

}