//
// Created by Massimo Perini on 10/07/2023.
//

#include "factorized_imputation_flight.h"
#include <iostream>
#include "../partition.h"
#include "../triple/Triple_sub.h"
#include "../triple/Triple_sum.h"
#include "../ML/lda.h"
#include "../ML/Regression.h"

//all the columns refer to fact table
void run_flight_partition_factorized_flight(duckdb::Connection &con, const std::vector<std::string> &con_columns_fact, const std::vector<std::string> &cat_columns, const std::vector<std::string> &con_columns_nulls, const std::vector<std::string> &cat_columns_nulls, const std::string &fact_table_name, size_t mice_iters){
    con.Query("ALTER TABLE "+fact_table_name+" ADD COLUMN n_nulls INTEGER DEFAULT 10;")->Print();
    std::string query = "CREATE TABLE rep AS SELECT ";

    //todo con_columns_fact need to be in the order of the query

    for (auto &col : con_columns_nulls)
        query += "CASE WHEN "+col+" IS NULL THEN 1 ELSE 0 END + ";
    for (auto &col : cat_columns_nulls)
        query += "CASE WHEN "+col+" IS NULL THEN 1 ELSE 0 END + ";
    query.pop_back();
    query.pop_back();
    con.Query(query+"::INTEGER FROM "+fact_table_name);
    //swap
    //works only with another vector (flat vector)
    con.Query("ALTER TABLE "+fact_table_name+" ALTER COLUMN n_nulls SET DEFAULT 10;")->Print();//not adding b, replace s with rep

    //con.Query("SELECT OP_CARRIER, COUNT(*) FROM join_table WHERE WHEELS_ON_HOUR is not null GROUP BY OP_CARRIER")->Print();

    build_list_of_uniq_categoricals(cat_columns, con, fact_table_name);
    //partition according to n. missing values
    auto begin = std::chrono::high_resolution_clock::now();
    std::vector<std::string> vals_partition = std::vector(con_columns_fact);
    vals_partition.push_back("index");
    partition(fact_table_name, con_columns_fact, con_columns_nulls, cat_columns, cat_columns_nulls, con, "index");
    auto end = std::chrono::high_resolution_clock::now();
    std::cout<<"Time partitioning fact table (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

    //run MICE
    //compute main cofactor
    std::string col_select = "";
    for(auto &col: con_columns_fact){
        col_select +=col+", ";
    }
    for(auto &col: cat_columns){
        col_select +=col+", ";
    }
    col_select.pop_back();
    col_select.pop_back();

    query = "SELECT triple_sum(multiply_triple(cnt2, cnt1)) FROM "
              "(SELECT SCHEDULE_ID, triple_sum_no_lift(DEP_DELAY, TAXI_OUT, TAXI_IN, ARR_DELAY, ACTUAL_ELAPSED_TIME, AIR_TIME, DEP_TIME_HOUR, DEP_TIME_MIN, WHEELS_OFF_HOUR, WHEELS_OFF_MIN, WHEELS_ON_HOUR, WHEELS_ON_MIN,  ARR_TIME_HOUR, ARR_TIME_MIN, MONTH_SIN, MONTH_COS, DAY_SIN, DAY_COS, WEEKDAY_SIN, WEEKDAY_COS, DIVERTED, EXTRA_DAY_ARR, EXTRA_DAY_DEP) AS cnt2 "
              " FROM "
              "( SELECT "+col_select+" FROM "+fact_table_name+"_complete_0 ";

    for (auto &con_col : con_columns_nulls)
        query += " UNION ALL SELECT "+col_select+" FROM "+fact_table_name+"_complete_"+con_col;
    for (auto &cat_col : cat_columns_nulls)
        query += " UNION ALL SELECT "+col_select+" FROM "+fact_table_name+"_complete_"+cat_col;

    query += " UNION ALL SELECT "+col_select+" FROM "+fact_table_name+"_complete_2) ";

    query += "as flight group by SCHEDULE_ID) as flight JOIN ("
              "    SELECT SCHEDULE_ID, multiply_triple(lift(CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN, OP_CARRIER), cnt) as cnt1"
              "    FROM schedule as schedule JOIN "
              "    (SELECT"
              "    ROUTE_ID, lift(DISTANCE) as cnt"
              "    FROM"
              "      route) as route"
              "      ON route.ROUTE_ID = schedule.ROUTE_ID) as schedule ON flight.SCHEDULE_ID = schedule.SCHEDULE_ID;";


    begin = std::chrono::high_resolution_clock::now();
    duckdb::Value full_triple = con.Query(query)->GetValue(0,0);
    end = std::chrono::high_resolution_clock::now();
    std::cout<<"Time full cofactor (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

    //start MICE

    for (int mice_iter =0; mice_iter<mice_iters;mice_iter++){
        //continuous cols
        for(auto &col_null : con_columns_nulls) {
            std::cout<<"\n\nColumn: "<<col_null<<"\n\n";
            //remove nulls
            std::string delta_query = "SELECT triple_sum(multiply_triple(cnt2, cnt1)) FROM "
                    "(SELECT SCHEDULE_ID, triple_sum_no_lift(DEP_DELAY, TAXI_OUT, TAXI_IN, ARR_DELAY, ACTUAL_ELAPSED_TIME, AIR_TIME, DEP_TIME_HOUR, DEP_TIME_MIN, WHEELS_OFF_HOUR, WHEELS_OFF_MIN, WHEELS_ON_HOUR, WHEELS_ON_MIN,  ARR_TIME_HOUR, ARR_TIME_MIN, MONTH_SIN, MONTH_COS, DAY_SIN, DAY_COS, WEEKDAY_SIN, WEEKDAY_COS, DIVERTED, EXTRA_DAY_ARR, EXTRA_DAY_DEP) AS cnt2 "
                    " FROM ((SELECT "+col_select+" FROM "+fact_table_name+"_complete_2 WHERE "+col_null+"_IS_NULL) ";
            delta_query += " UNION ALL SELECT "+col_select+" FROM "+fact_table_name+"_complete_"+col_null;
            delta_query += ") as flight group by SCHEDULE_ID) as flight JOIN ("
                     "    SELECT SCHEDULE_ID, multiply_triple(lift(CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN, OP_CARRIER), cnt) as cnt1"
                     "    FROM schedule as schedule JOIN "
                     "    (SELECT"
                     "    ROUTE_ID, lift(DISTANCE) as cnt"
                     "    FROM"
                     "      route) as route"
                     "      ON route.ROUTE_ID = schedule.ROUTE_ID) as schedule ON flight.SCHEDULE_ID = schedule.SCHEDULE_ID;";


            begin = std::chrono::high_resolution_clock::now();
            duckdb::Value null_triple = con.Query(delta_query)->GetValue(0,0);
            end = std::chrono::high_resolution_clock::now();
            std::cout<<"Time delta cofactor (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

            duckdb::Value train_triple = Triple::subtract_triple(full_triple, null_triple);

            //train
            auto it = std::find(con_columns_fact.begin(), con_columns_fact.end(), col_null);
            size_t label_index = it - con_columns_fact.begin();
            std::cout<<"Label index "<<label_index<<"\n";

            std::vector <double> params = Triple::ridge_linear_regression(train_triple, label_index, 0.001, 0, 1000);
            //predict query

            std::vector<std::string> con_columns_full = {"DEP_DELAY", "TAXI_OUT", "TAXI_IN", "ARR_DELAY", "ACTUAL_ELAPSED_TIME",
                                                         "AIR_TIME", "DEP_TIME_HOUR", "DEP_TIME_MIN", "WHEELS_OFF_HOUR", "WHEELS_OFF_MIN", "WHEELS_ON_HOUR", "WHEELS_ON_MIN",  "ARR_TIME_HOUR",
                                                         "ARR_TIME_MIN", "MONTH_SIN", "MONTH_COS", "DAY_SIN", "DAY_COS", "WEEKDAY_SIN", "WEEKDAY_COS",
                                                         "CRS_DEP_HOUR", "CRS_DEP_MIN", "CRS_ARR_HOUR", "CRS_ARR_MIN", "DISTANCE"};

            std::vector<std::string> cat_columns_full = {"DIVERTED", "EXTRA_DAY_ARR", "EXTRA_DAY_DEP", "OP_CARRIER"};


            std::string new_val = "";
            for(size_t i=0; i < con_columns_full.size(); i++) {
                if (i==label_index)
                    continue;
                new_val+= "(" + std::to_string((float)params[i]) + " * " + con_columns_full[i] + ")+";
            }

            std::string cat_columns_query;
            query_categorical_num(cat_columns_full, cat_columns_query, std::vector<float>(params.begin() + con_columns_full.size(), params.end()));
            new_val+=cat_columns_query;
            //update 1 missing value
            std::string update_query = "CREATE TABLE rep AS SELECT "+new_val+" AS new_vals FROM "
                                    " ( SELECT * FROM "+fact_table_name+"_complete_"+col_null+
                                    ") as flight JOIN ("
                     "    SELECT SCHEDULE_ID, CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN, OP_CARRIER"
                     "    FROM schedule as schedule JOIN "
                     "    (SELECT ROUTE_ID, DISTANCE as cnt"
                     "    FROM route) as route"
                     "      ON route.ROUTE_ID = schedule.ROUTE_ID) as schedule ON flight.SCHEDULE_ID = schedule.SCHEDULE_ID ORDER BY index;";

            begin = std::chrono::high_resolution_clock::now();
            con.Query(update_query);
            con.Query("ALTER TABLE "+fact_table_name+"_complete_"+col_null+" ALTER COLUMN "+col_null+" SET DEFAULT 10;")->Print();//not adding b, replace s with rep
            end = std::chrono::high_resolution_clock::now();
            std::cout<<"Time updating ==1 partition (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

            //update 2 missing values

            std::string select_stmt = "CREATE TABLE rep AS SELECT CASE WHEN "+col_null+"_IS_NULL THEN "+new_val+" ELSE "+col_null + "AS new_vals FROM "
                            " ( SELECT * FROM "+fact_table_name+"_complete_2) as flight LEFT JOIN ("
                            "    SELECT SCHEDULE_ID, CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN, OP_CARRIER"
                            "    FROM schedule as schedule JOIN "
                            "    (SELECT ROUTE_ID, DISTANCE as cnt"
                            "    FROM route) as route"
                            "      ON route.ROUTE_ID = schedule.ROUTE_ID) as schedule ON "+col_null+"_IS_NULL AND flight.SCHEDULE_ID = schedule.SCHEDULE_ID ORDER BY index";



            std::cout<<update_query+"\n";
            begin = std::chrono::high_resolution_clock::now();
            con.Query(select_stmt);
            con.Query("ALTER TABLE "+fact_table_name+"_complete_2 ALTER COLUMN "+col_null+" SET DEFAULT 10;")->Print();//not adding b, replace s with rep
            end = std::chrono::high_resolution_clock::now();
            std::cout<<"Time updating >=2 partition (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";
            //con.Query("SELECT * from "+table_name+"_complete_"+col_null+" LIMIT 100")->Print();
            //con.Query("SELECT * from "+table_name+"_complete_2 LIMIT 100")->Print();


            //recompute cofactor
            begin = std::chrono::high_resolution_clock::now();
            null_triple = con.Query(delta_query)->GetValue(0,0);
            end = std::chrono::high_resolution_clock::now();
            std::cout<<"Time delta cofactor (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";
            full_triple = Triple::sum_triple(train_triple, null_triple);
        }



        std::cout<<"Starting categorical variables..."<<std::endl;
        for(auto &col_null : cat_columns_nulls) {
            std::cout<<"\n\nColumn: "<<col_null<<"\n\n";
            //remove nulls

            std::string delta_query = "SELECT triple_sum(multiply_triple(cnt2, cnt1)) FROM "
                                      "(SELECT SCHEDULE_ID, triple_sum_no_lift(DEP_DELAY, TAXI_OUT, TAXI_IN, ARR_DELAY, ACTUAL_ELAPSED_TIME, AIR_TIME, DEP_TIME_HOUR, DEP_TIME_MIN, WHEELS_OFF_HOUR, WHEELS_OFF_MIN, WHEELS_ON_HOUR, WHEELS_ON_MIN,  ARR_TIME_HOUR, ARR_TIME_MIN, MONTH_SIN, MONTH_COS, DAY_SIN, DAY_COS, WEEKDAY_SIN, WEEKDAY_COS, DIVERTED, EXTRA_DAY_ARR, EXTRA_DAY_DEP) AS cnt2 "
                                      " FROM ((SELECT "+col_select+" FROM "+fact_table_name+"_complete_2 WHERE "+col_null+"_IS_NULL) ";
            delta_query += " UNION ALL SELECT "+col_select+" FROM "+fact_table_name+"_complete_"+col_null;
            delta_query += ") as flight group by SCHEDULE_ID) as flight JOIN ("
                           "    SELECT SCHEDULE_ID, multiply_triple(lift(CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN, OP_CARRIER), cnt) as cnt1"
                           "    FROM schedule as schedule JOIN "
                           "    (SELECT"
                           "    ROUTE_ID, lift(DISTANCE) as cnt"
                           "    FROM"
                           "      route) as route"
                           "      ON route.ROUTE_ID = schedule.ROUTE_ID) as schedule ON flight.SCHEDULE_ID = schedule.SCHEDULE_ID;";


            begin = std::chrono::high_resolution_clock::now();
            duckdb::Value null_triple = con.Query(delta_query)->GetValue(0,0);
            end = std::chrono::high_resolution_clock::now();
            std::cout<<"Time delta cofactor (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

            duckdb::Value train_triple = Triple::subtract_triple(full_triple, null_triple);
            //train_triple.Print();

            //train
            auto it = std::find(cat_columns.begin(), cat_columns.end(), col_null);
            size_t label_index = it - cat_columns.begin();
            std::cout<<"Categorical label index "<<label_index<<"\n";
            begin = std::chrono::high_resolution_clock::now();
            auto train_params =  lda_train(train_triple, label_index, 0.4);
            end = std::chrono::high_resolution_clock::now();
            std::cout<<"Train Time (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";
            //predict query

            std::vector<std::string> con_columns_full = {"DEP_DELAY", "TAXI_OUT", "TAXI_IN", "ARR_DELAY", "ACTUAL_ELAPSED_TIME",
                                                         "AIR_TIME", "DEP_TIME_HOUR", "DEP_TIME_MIN", "WHEELS_OFF_HOUR", "WHEELS_OFF_MIN", "WHEELS_ON_HOUR", "WHEELS_ON_MIN",  "ARR_TIME_HOUR",
                                                         "ARR_TIME_MIN", "MONTH_SIN", "MONTH_COS", "DAY_SIN", "DAY_COS", "WEEKDAY_SIN", "WEEKDAY_COS",
                                                         "CRS_DEP_HOUR", "CRS_DEP_MIN", "CRS_ARR_HOUR", "CRS_ARR_MIN", "DISTANCE"};

            std::vector<std::string> cat_columns_full = {"DIVERTED", "EXTRA_DAY_ARR", "EXTRA_DAY_DEP", "OP_CARRIER"};


            /////----

            std::string cat_columns_query;
            std::string predict_column_query;
            query_categorical(cat_columns, label_index, cat_columns_query, predict_column_query);
            //impute
            std::string delimiter = ", ";
            std::stringstream  s;
            copy(con_columns_fact.begin(),con_columns_fact.end(), std::ostream_iterator<std::string>(s,delimiter.c_str()));
            std::string num_cols_query = s.str();
            num_cols_query.pop_back();
            num_cols_query.pop_back();
            std::string select_stmt = (" list_extract("+predict_column_query+", predict_lda("+train_params.ToString()+"::FLOAT[], "+num_cols_query+", "+cat_columns_query+")+1)");

            //update 1 missing value
            std::string update_query = "CREATE TABLE rep AS SELECT "+select_stmt+" AS new_vals FROM "
                                       " ( SELECT * FROM "+fact_table_name+"_complete_"+col_null+
                                       ") as flight JOIN ("
                                       "    SELECT SCHEDULE_ID, CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN, OP_CARRIER"
                                       "    FROM schedule as schedule JOIN "
                                       "    (SELECT ROUTE_ID, DISTANCE as cnt"
                                       "    FROM route) as route"
                                       "      ON route.ROUTE_ID = schedule.ROUTE_ID) as schedule ON flight.SCHEDULE_ID = schedule.SCHEDULE_ID order by index;";

            begin = std::chrono::high_resolution_clock::now();
            con.Query(update_query);
            con.Query("ALTER TABLE "+fact_table_name+"_complete_"+col_null+" ALTER COLUMN "+col_null+" SET DEFAULT 10;")->Print();//not adding b, replace s with rep
            end = std::chrono::high_resolution_clock::now();
            std::cout<<"Time updating ==1 partition (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

            //update 2 missing values

            update_query = "CREATE TABLE rep AS SELECT CASE WHEN "+col_null+"_IS_NULL THEN "+select_stmt+" ELSE "+col_null + "AS new_vals FROM "
                             " ( SELECT * FROM "+fact_table_name+"_complete_2 "
                             ") as flight LEFT JOIN ("
                             "    SELECT SCHEDULE_ID, CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN, OP_CARRIER"
                             "    FROM schedule as schedule JOIN "
                             "    (SELECT ROUTE_ID, DISTANCE as cnt"
                             "    FROM route) as route"
                             "      ON route.ROUTE_ID = schedule.ROUTE_ID) as schedule ON "+col_null+"_IS_NULL AND flight.SCHEDULE_ID = schedule.SCHEDULE_ID";
            std::cout<<update_query+"\n";
            begin = std::chrono::high_resolution_clock::now();
            con.Query(update_query);
            con.Query("ALTER TABLE "+fact_table_name+"_complete_2 ALTER COLUMN "+col_null+" SET DEFAULT 10;")->Print();//not adding b, replace s with rep
            end = std::chrono::high_resolution_clock::now();
            std::cout<<"Time updating >=2 partition (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";
            //con.Query("SELECT * from "+table_name+"_complete_"+col_null+" LIMIT 100")->Print();
            //con.Query("SELECT * from "+table_name+"_complete_2 LIMIT 100")->Print();


            //recompute cofactor
            begin = std::chrono::high_resolution_clock::now();
            null_triple = con.Query(delta_query)->GetValue(0,0);
            end = std::chrono::high_resolution_clock::now();
            std::cout<<"Time delta cofactor (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";
            full_triple = Triple::sum_triple(train_triple, null_triple);
        }
    }
}