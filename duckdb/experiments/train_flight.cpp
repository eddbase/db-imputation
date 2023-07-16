

#include "train_flight.h"

#include "../triple/helper.h"
#include "../ML/Regression.h"
#include <iostream>
#include <duckdb.hpp>
#include <iterator>

namespace Flight {
    void import_data(duckdb::Connection &con, const std::string &path) {
        con.Query("CREATE TABLE route (ROUTE_ID INTEGER PRIMARY KEY, ORIGIN INT, DEST INT, DISTANCE FLOAT);");
        con.Query(
                "CREATE TABLE schedule (SCHEDULE_ID INTEGER PRIMARY KEY, OP_CARRIER INT, CRS_DEP_HOUR FLOAT, CRS_DEP_MIN FLOAT, CRS_ARR_HOUR FLOAT, CRS_ARR_MIN FLOAT, ROUTE_ID INTEGER);");
        con.Query(
                "CREATE TABLE flight (id INTEGER PRIMARY KEY, DEP_DELAY FLOAT, TAXI_OUT FLOAT, TAXI_IN FLOAT, ARR_DELAY FLOAT, DIVERTED INTEGER, ACTUAL_ELAPSED_TIME FLOAT, AIR_TIME FLOAT, DEP_TIME_HOUR FLOAT, DEP_TIME_MIN FLOAT, \n"
                "WHEELS_OFF_HOUR FLOAT, WHEELS_OFF_MIN FLOAT, WHEELS_ON_HOUR FLOAT, WHEELS_ON_MIN FLOAT, ARR_TIME_HOUR FLOAT, ARR_TIME_MIN FLOAT, MONTH_SIN FLOAT, MONTH_COS FLOAT, DAY_SIN FLOAT, DAY_COS FLOAT, WEEKDAY_SIN \n"
                "FLOAT, WEEKDAY_COS FLOAT, EXTRA_DAY_ARR INTEGER, EXTRA_DAY_DEP INTEGER, SCHEDULE_ID INTEGER);");

        con.Query("COPY route FROM '" + path +
                  "/schedule_dataset_postgres.csv' (FORMAT CSV, AUTO_DETECT TRUE , nullstr '', DELIMITER ',')");
        con.Query("COPY schedule FROM '" + path +
                  "/airline_dataset_postgres.csv' (FORMAT CSV, AUTO_DETECT TRUE , nullstr '', DELIMITER ',')");
        con.Query("COPY flight FROM '" + path +
                  "/airlines_data_dataset_postgres.csv' (FORMAT CSV, AUTO_DETECT TRUE , nullstr '', DELIMITER ',')");
    }


    void train_mat_sql_lift(const std::string &path, bool materialized) {
        duckdb::DuckDB db(":memory:");
        duckdb::Connection con(db);
        //import data
        import_data(con, path);

        std::vector<std::string> con_columns = {"CRS_DEP_HOUR", "CRS_DEP_MIN", "CRS_ARR_HOUR", "CRS_ARR_MIN",
                                                "DISTANCE", "DEP_DELAY", "TAXI_OUT", "TAXI_IN", "ARR_DELAY",
                                                "ACTUAL_ELAPSED_TIME", "AIR_TIME", "DEP_TIME_HOUR", "DEP_TIME_MIN",
                                                "WHEELS_OFF_HOUR", "WHEELS_OFF_MIN", "WHEELS_ON_HOUR", "WHEELS_ON_MIN",
                                                "ARR_TIME_HOUR", "ARR_TIME_MIN", "MONTH_SIN", "MONTH_COS", "DAY_SIN",
                                                "DAY_COS", "WEEKDAY_SIN", "WEEKDAY_COS"};

        if (materialized) {
            auto begin = std::chrono::high_resolution_clock::now();
            std::string aaa = "CREATE TABLE tbl_flight AS (SELECT * FROM Route JOIN schedule USING (ROUTE_ID) JOIN flight USING (SCHEDULE_ID))";
            con.Query(aaa);
            auto end = std::chrono::high_resolution_clock::now();
            std::cout << "Time JOIN: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                      << "\n";
        }

        std::string query = "SELECT {'N': SUM(1), 'lin_agg':LIST_VALUE(";
        for (auto &con_col: con_columns) {
            query += "SUM(" + con_col + "), ";
        }
        query.pop_back();
        query.pop_back();

        query += "), 'quad_agg': LIST_VALUE(";

        for (size_t i = 0; i < con_columns.size(); i++) {
            for (size_t j = i; j < con_columns.size(); j++) {
                query += "SUM(" + con_columns[i] + "*" + con_columns[j] + "), ";
            }
        }
        query.pop_back();
        query.pop_back();
        query += "), 'lin_cat':LIST_VALUE(), 'quad_num_cat':LIST_VALUE(), 'quad_cat':LIST_VALUE()} FROM ";
        if (materialized)
            query += "tbl_flight";
        else
            query += "Route JOIN schedule USING (ROUTE_ID) JOIN flight USING (SCHEDULE_ID)";

        auto begin = std::chrono::high_resolution_clock::now();
        auto train_triple = con.Query(query)->GetValue(0, 0);
        auto end = std::chrono::high_resolution_clock::now();
        std::cout << "Time cofactor: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << "\n";
        begin = std::chrono::high_resolution_clock::now();
        std::vector<double> params = Triple::ridge_linear_regression(train_triple, 0, 0.001, 0, 1000);
        end = std::chrono::high_resolution_clock::now();
        std::cout << "Time train: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << "\n";

    }

    void train_mat_custom_lift(const std::string &path, bool materialized, bool categorical) {
        duckdb::DuckDB db(":memory:");
        duckdb::Connection con(db);
        //import data

        import_data(con, path);

        std::vector<std::string> con_columns = {"CRS_DEP_HOUR", "CRS_DEP_MIN", "CRS_ARR_HOUR", "CRS_ARR_MIN",
                                                "DISTANCE", "DEP_DELAY", "TAXI_OUT", "TAXI_IN", "ARR_DELAY",
                                                "ACTUAL_ELAPSED_TIME", "AIR_TIME", "DEP_TIME_HOUR", "DEP_TIME_MIN",
                                                "WHEELS_OFF_HOUR", "WHEELS_OFF_MIN", "WHEELS_ON_HOUR", "WHEELS_ON_MIN",
                                                "ARR_TIME_HOUR", "ARR_TIME_MIN", "MONTH_SIN", "MONTH_COS", "DAY_SIN",
                                                "DAY_COS", "WEEKDAY_SIN", "WEEKDAY_COS"};
        std::vector<std::string> cat_columns = {"DIVERTED", "EXTRA_DAY_ARR", "EXTRA_DAY_DEP", "OP_CARRIER"};
        if (categorical)
            Triple::register_functions(*con.context, {con_columns.size()}, {cat_columns.size()});
        else
            Triple::register_functions(*con.context, {con_columns.size()}, {0});

        if (materialized) {
            auto begin = std::chrono::high_resolution_clock::now();
            std::string aaa = "CREATE TABLE tbl_flight AS (SELECT * FROM Route JOIN schedule USING (ROUTE_ID) JOIN flight USING (SCHEDULE_ID))";
            con.Query(aaa);
            auto end = std::chrono::high_resolution_clock::now();
            std::cout << "Time JOIN: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                      << "\n";
        }

        std::string query = "SELECT triple_sum_no_lift(";
        std::string delimiter = ", ";
        std::stringstream s;
        copy(con_columns.begin(), con_columns.end(), std::ostream_iterator<std::string>(s, delimiter.c_str()));
        if (categorical)
            copy(cat_columns.begin(), cat_columns.end(), std::ostream_iterator<std::string>(s, delimiter.c_str()));
        std::string num_cols_query = s.str();
        num_cols_query.pop_back();
        num_cols_query.pop_back();

        query += num_cols_query + ") FROM ";
        if (materialized)
            query += "tbl_flight";
        else
            query += "Route JOIN schedule USING (ROUTE_ID) JOIN flight USING (SCHEDULE_ID)";

        auto begin = std::chrono::high_resolution_clock::now();
        auto train_triple = con.Query(query)->GetValue(0, 0);
        auto end = std::chrono::high_resolution_clock::now();
        std::cout << "Time cofactor: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << "\n";
        begin = std::chrono::high_resolution_clock::now();
        std::vector<double> params = Triple::ridge_linear_regression(train_triple, 0, 0.001, 0, 1000);
        end = std::chrono::high_resolution_clock::now();
        std::cout << "Time train: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << "\n";

    }

    void train_factorized(const std::string &path, bool categorical) {
        duckdb::DuckDB db(":memory:");
        duckdb::Connection con(db);
        //import data
        import_data(con, path);

        Triple::register_functions(*con.context, {20, 4, 1, 20}, {3, 1, 0, 0});//con+cat, con only
        //20,3 4,1 1,0 20,0
        std::string aaa;
        //factorized
        if (categorical) {
            aaa = "SELECT triple_sum(multiply_triple(cnt1, cnt2)) FROM "
                  "(SELECT SCHEDULE_ID, triple_sum_no_lift4(DEP_DELAY, TAXI_OUT, TAXI_IN, ARR_DELAY, ACTUAL_ELAPSED_TIME, AIR_TIME, DEP_TIME_HOUR, DEP_TIME_MIN, WHEELS_OFF_HOUR, WHEELS_OFF_MIN, WHEELS_ON_HOUR, WHEELS_ON_MIN,  ARR_TIME_HOUR, ARR_TIME_MIN, MONTH_SIN, MONTH_COS, DAY_SIN, DAY_COS, WEEKDAY_SIN, WEEKDAY_COS, DIVERTED, EXTRA_DAY_ARR, EXTRA_DAY_DEP) AS cnt2 "
                  " FROM flight as flight group by SCHEDULE_ID) as flight JOIN ("
                  "    SELECT SCHEDULE_ID, multiply_triple(lift(CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN, OP_CARRIER), cnt) as cnt1"
                  "    FROM schedule as schedule JOIN "
                  "    (SELECT"
                  "    ROUTE_ID, lift(DISTANCE) as cnt"
                  "    FROM"
                  "      route) as route"
                  "      ON route.ROUTE_ID = schedule.ROUTE_ID) as schedule ON flight.SCHEDULE_ID = schedule.SCHEDULE_ID;";

        } else {
            aaa = "SELECT triple_sum(multiply_triple(cnt1, cnt2)) FROM "
                  " (SELECT SCHEDULE_ID, triple_sum_no_lift3(DEP_DELAY, TAXI_OUT, TAXI_IN, ARR_DELAY, ACTUAL_ELAPSED_TIME, AIR_TIME, DEP_TIME_HOUR, DEP_TIME_MIN, WHEELS_OFF_HOUR, WHEELS_OFF_MIN, WHEELS_ON_HOUR, WHEELS_ON_MIN, ARR_TIME_HOUR, ARR_TIME_MIN, MONTH_SIN, MONTH_COS, DAY_SIN, DAY_COS, WEEKDAY_SIN, WEEKDAY_COS) AS cnt2 "
                  " FROM flight as flight group by SCHEDULE_ID) as flight JOIN ("
                  "    SELECT SCHEDULE_ID, multiply_triple(lift(CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN), cnt) as cnt1"
                  "    FROM schedule as schedule JOIN "
                  "    (SELECT"
                  "    ROUTE_ID, lift(DISTANCE) as cnt"
                  "    FROM"
                  "      route) as route"
                  "      ON route.ROUTE_ID = schedule.ROUTE_ID) as schedule ON flight.SCHEDULE_ID = schedule.SCHEDULE_ID;";
        }        auto begin = std::chrono::high_resolution_clock::now();
        duckdb::Value train_triple = con.Query(aaa)->GetValue(0, 0);
        auto end = std::chrono::high_resolution_clock::now();

        std::cout << "Time cofactor: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << "\n";
        begin = std::chrono::high_resolution_clock::now();
        std::vector<double> params = Triple::ridge_linear_regression(train_triple, 0, 0.001, 0, 1000);
        end = std::chrono::high_resolution_clock::now();
        std::cout << "Time train: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << "\n";
    }

    void test(const std::string &path) {
        {
            duckdb::DuckDB db(":memory:");
            duckdb::Connection con(db);
            //import data
            import_data(con, path);
            std::cout << "\n\n\nImported data\n\n\n";
            Triple::register_functions(*con.context, {20, 4, 1, 20}, {3, 1, 0, 0});//con+cat, con only
            //20,3 4,1 1,0 20,0
            std::string aaa;
            //factorized
            //con.Query("SET threads TO 1;");
            aaa = "SELECT triple_sum_no_lift4(DEP_DELAY, TAXI_OUT, TAXI_IN, ARR_DELAY, ACTUAL_ELAPSED_TIME, AIR_TIME, DEP_TIME_HOUR, DEP_TIME_MIN, WHEELS_OFF_HOUR, WHEELS_OFF_MIN, WHEELS_ON_HOUR, WHEELS_ON_MIN, ARR_TIME_HOUR, ARR_TIME_MIN, MONTH_SIN, MONTH_COS, DAY_SIN, DAY_COS, WEEKDAY_SIN, WEEKDAY_COS, DIVERTED, EXTRA_DAY_ARR, EXTRA_DAY_DEP) "
                  "  FROM flight";
            //aaa = "SELECT SUM(DEP_DELAY) FROM flight";
            std::cout << "START TIMER\n";
            auto begin = std::chrono::high_resolution_clock::now();
            con.Query(aaa);//->Print();
            auto end = std::chrono::high_resolution_clock::now();
            std::cout << "Time part 1: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                      << "\n";
            return;
        }
    }

}