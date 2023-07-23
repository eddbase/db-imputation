#include "factorized_imputation_flight.h"
#include <iostream>
#include "../partition.h"
#include "../triple/Triple_sub.h"
#include "../triple/Triple_sum.h"
#include "../ML/lda.h"
#include "../ML/Regression.h"
#include "../triple/helper.h"

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

//all the columns refer to fact table
void run_flight_partition_factorized_flight(const std::string &path, const std::string &fact_table_name,
                                            size_t mice_iters){

    duckdb::DuckDB db(":memory:");
    duckdb::Connection con(db);
    //import data

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
                                                 "DAY_COS", "WEEKDAY_SIN", "WEEKDAY_COS", "CRS_DEP_HOUR", "CRS_DEP_MIN", "CRS_ARR_HOUR", "CRS_ARR_MIN",
                                                 "DISTANCE"};

    std::vector<std::string> cat_columns_join = {"DIVERTED", "EXTRA_DAY_ARR", "EXTRA_DAY_DEP", "OP_CARRIER"};
    Triple::register_functions(*con.context, {20}, {3});//con+cat, con only

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

    //con.Query("SET threads TO 1;");
    std::vector<std::pair<std::string, std::string>> cat_col_info;
    cat_col_info.push_back(std::make_pair("DIVERTED", "join_table"));
    cat_col_info.push_back(std::make_pair("EXTRA_DAY_ARR", "join_table"));
    cat_col_info.push_back(std::make_pair("EXTRA_DAY_DEP", "join_table"));
    cat_col_info.push_back(std::make_pair("OP_CARRIER", "schedule"));
    build_list_of_uniq_categoricals(cat_col_info, con);

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

    query = "SELECT triple_sum(multiply_triple(cnt2, cnt1)) FROM "
              "(SELECT SCHEDULE_ID, triple_sum_no_lift(DEP_DELAY, TAXI_OUT, TAXI_IN, ARR_DELAY, ACTUAL_ELAPSED_TIME, AIR_TIME, DEP_TIME_HOUR, DEP_TIME_MIN, WHEELS_OFF_HOUR, WHEELS_OFF_MIN, WHEELS_ON_HOUR, WHEELS_ON_MIN,  ARR_TIME_HOUR, ARR_TIME_MIN, MONTH_SIN, MONTH_COS, DAY_SIN, DAY_COS, WEEKDAY_SIN, WEEKDAY_COS, DIVERTED, EXTRA_DAY_ARR, EXTRA_DAY_DEP) AS cnt2 "
              " FROM "
              "( SELECT "+col_select+" FROM "+fact_table_name+"_complete_0 ";
    for (auto &con_col : con_columns_fact_null)
        query += " UNION ALL SELECT "+col_select+" FROM "+fact_table_name+"_complete_"+con_col;
    for (auto &cat_col : cat_columns_fact_null)
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

    std::cout<<query<<"\n";
    //con.Query("SET threads TO 10;");
    begin = std::chrono::high_resolution_clock::now();
    duckdb::Value full_triple = con.Query(query)->GetValue(0,0);
    end = std::chrono::high_resolution_clock::now();
    std::cout<<"Time full cofactor (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

    //start MICE

    for (int mice_iter =0; mice_iter<mice_iters;mice_iter++){
        //continuous cols
        for(auto &col_null : con_columns_fact_null) {
            std::cout<<"\n\nColumn: "<<col_null<<"\n\n";
            //con.Query("SET threads TO 10;");
            //remove nulls
            std::string delta_query = "SELECT triple_sum(multiply_triple(cnt2, cnt1)) FROM "
                                      "(SELECT SCHEDULE_ID, triple_sum_no_lift(DEP_DELAY, TAXI_OUT, TAXI_IN, ARR_DELAY, ACTUAL_ELAPSED_TIME, AIR_TIME, DEP_TIME_HOUR, DEP_TIME_MIN, WHEELS_OFF_HOUR, WHEELS_OFF_MIN, WHEELS_ON_HOUR, WHEELS_ON_MIN,  ARR_TIME_HOUR, ARR_TIME_MIN, MONTH_SIN, MONTH_COS, DAY_SIN, DAY_COS, WEEKDAY_SIN, WEEKDAY_COS, DIVERTED, EXTRA_DAY_ARR, EXTRA_DAY_DEP) AS cnt2 "
                                      " FROM (SELECT "+col_select+" FROM "+fact_table_name+"_complete_"+col_null+
            " UNION ALL (SELECT "+col_select+" FROM "+fact_table_name+"_complete_2 WHERE "+col_null+"_IS_NULL)) "
             "as flight group by SCHEDULE_ID) as flight JOIN ("
                     "    SELECT SCHEDULE_ID, multiply_triple(lift(CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN, OP_CARRIER), cnt) as cnt1"
                     "    FROM schedule as schedule JOIN "
                     "    (SELECT"
                     "    ROUTE_ID, lift(DISTANCE) as cnt"
                     "    FROM"
                     "      route) as route"
                     "      ON route.ROUTE_ID = schedule.ROUTE_ID) as schedule ON flight.SCHEDULE_ID = schedule.SCHEDULE_ID;";

            std::cout<<delta_query<<"\n";
            begin = std::chrono::high_resolution_clock::now();
            duckdb::Value null_triple = con.Query(delta_query)->GetValue(0,0);
            end = std::chrono::high_resolution_clock::now();
            std::cout<<"Time delta cofactor (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";
            duckdb::Value train_triple = Triple::subtract_triple(full_triple, null_triple);

            //train
            auto it = std::find(con_columns_fact.begin(), con_columns_fact.end(), col_null);
            size_t label_index = it - con_columns_fact.begin();
            std::cout<<"Label index "<<label_index<<"\n";

            std::vector <double> params = Triple::ridge_linear_regression(train_triple, label_index, 0.001, 0, 1000, true);
            //predict query

            std::string new_val = std::to_string((float) params[0])+" + (";
            for (size_t i = 0; i < con_columns_join.size(); i++) {
                if (i == label_index)
                    continue;
                new_val += "(" + std::to_string((float) params[i+1]) + " * " + con_columns_join[i] + ")+";
            }

            if (cat_columns_join.empty())
                new_val.pop_back();

            std::string cat_columns_query;
            query_categorical_num(cat_columns_join, cat_columns_query,
                                  std::vector<float>(params.begin() + con_columns_join.size(), params.end()-1));
            new_val += cat_columns_query + "+(random()*"+ std::to_string(params[params.size()-1])+"))::FLOAT";
            //update 1 missing value
            //con.Query("SET threads TO 1;");
            std::string update_query = "CREATE TABLE rep AS SELECT "+new_val+" AS new_vals FROM "
                                    +fact_table_name+"_complete_"+col_null+
                                    " as flight JOIN ("
                     "    SELECT * FROM schedule as schedule JOIN route"
                     "      ON route.ROUTE_ID = schedule.ROUTE_ID) as schedule ON flight.SCHEDULE_ID = schedule.SCHEDULE_ID ORDER BY id;";
            begin = std::chrono::high_resolution_clock::now();
            std::cout<<update_query<<std::endl;

            con.Query(update_query)->Print();

            //con.Query("SELECT * FROM rep LIMIT 10000;")->Print();

            con.Query("ALTER TABLE "+fact_table_name+"_complete_"+col_null+" ALTER COLUMN "+col_null+" SET DEFAULT 10;")->Print();//not adding b, replace s with rep
            end = std::chrono::high_resolution_clock::now();
            std::cout<<"Time updating ==1 partition (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

            //update 2 missing values

            std::string select_stmt = "CREATE TABLE rep AS SELECT CASE WHEN "+col_null+"_IS_NULL THEN "+new_val+" ELSE "+col_null + " END AS new_vals FROM "+fact_table_name+"_complete_2"+
                            " AS flight JOIN ("+
                            "    SELECT SCHEDULE_ID, CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN, OP_CARRIER, route.DISTANCE "
                            "    FROM schedule as schedule JOIN route "
                            "      ON route.ROUTE_ID = schedule.ROUTE_ID) as schedule ON flight.SCHEDULE_ID = schedule.SCHEDULE_ID ORDER BY id";

            std::cout<<select_stmt+"\n";
            begin = std::chrono::high_resolution_clock::now();
            con.Query(select_stmt)->Print();
            con.Query("ALTER TABLE "+fact_table_name+"_complete_2 ALTER COLUMN "+col_null+" SET DEFAULT 10;")->Print();//not adding b, replace s with rep
            end = std::chrono::high_resolution_clock::now();
            std::cout<<"Time updating >=2 partition (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

            //recompute cofactor
            begin = std::chrono::high_resolution_clock::now();
            null_triple = con.Query(delta_query)->GetValue(0,0);
            end = std::chrono::high_resolution_clock::now();
            std::cout<<"Time delta cofactor (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";
            full_triple = Triple::sum_triple(train_triple, null_triple);
        }



        std::cout<<"Starting categorical variables..."<<std::endl;
        for(auto &col_null : cat_columns_fact_null) {
            std::cout<<"\n\nColumn: "<<col_null<<"\n\n";
            //remove nulls

            //con.Query("SET threads TO 10;");
            std::string delta_query = "SELECT triple_sum(multiply_triple(cnt2, cnt1)) FROM "
                                      "(SELECT SCHEDULE_ID, triple_sum_no_lift(DEP_DELAY, TAXI_OUT, TAXI_IN, ARR_DELAY, ACTUAL_ELAPSED_TIME, AIR_TIME, DEP_TIME_HOUR, DEP_TIME_MIN, WHEELS_OFF_HOUR, WHEELS_OFF_MIN, WHEELS_ON_HOUR, WHEELS_ON_MIN,  ARR_TIME_HOUR, ARR_TIME_MIN, MONTH_SIN, MONTH_COS, DAY_SIN, DAY_COS, WEEKDAY_SIN, WEEKDAY_COS, DIVERTED, EXTRA_DAY_ARR, EXTRA_DAY_DEP) AS cnt2 "
                                      " FROM (SELECT "+col_select+" FROM "+fact_table_name+"_complete_"+col_null+
                                      " UNION ALL (SELECT "+col_select+" FROM "+fact_table_name+"_complete_2 WHERE "+col_null+"_IS_NULL)) "
                                      "as flight group by SCHEDULE_ID) as flight JOIN ("
                                      "    SELECT SCHEDULE_ID, multiply_triple(lift(CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN, OP_CARRIER), cnt) as cnt1"
                                      "    FROM schedule as schedule JOIN "
                                      "    (SELECT ROUTE_ID, lift(DISTANCE) as cnt FROM route) as route ON route.ROUTE_ID = schedule.ROUTE_ID) as schedule ON flight.SCHEDULE_ID = schedule.SCHEDULE_ID;";


            begin = std::chrono::high_resolution_clock::now();
            duckdb::Value null_triple = con.Query(delta_query)->GetValue(0,0);
            end = std::chrono::high_resolution_clock::now();
            std::cout<<"Time delta cofactor (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

            duckdb::Value train_triple = Triple::subtract_triple(full_triple, null_triple);
            //train_triple.Print();

            //train (fact table is the first one in terms of columns)
            auto it = std::find(cat_columns_fact.begin(), cat_columns_fact.end(), col_null);
            size_t label_index = it - cat_columns_fact.begin();
            std::cout<<"Categorical label index "<<label_index<<"\n";
            begin = std::chrono::high_resolution_clock::now();
            auto train_params =  lda_train(train_triple, label_index, 0.4);
            end = std::chrono::high_resolution_clock::now();
            std::cout<<"Train Time (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";
            //predict query

            /////----

            std::string cat_columns_query;
            std::string predict_column_query;
            query_categorical(cat_columns_join, label_index, cat_columns_query, predict_column_query);
            //impute
            std::string delimiter = ", ";
            std::stringstream  s;
            copy(con_columns_join.begin(),con_columns_join.end(), std::ostream_iterator<std::string>(s,delimiter.c_str()));
            std::string num_cols_query = s.str();
            num_cols_query.pop_back();
            num_cols_query.pop_back();
            std::string select_stmt = (" list_extract("+predict_column_query+", predict_lda("+train_params.ToString()+"::FLOAT[], "+num_cols_query+", "+cat_columns_query+")+1)");

            //update 1 missing value
            //con.Query("SET threads TO 1;");
            std::string update_query = "CREATE TABLE rep AS SELECT "+select_stmt+" AS new_vals FROM "
                                       " ( SELECT * FROM "+fact_table_name+"_complete_"+col_null+
                                       ") as flight JOIN ("
                                       "    SELECT SCHEDULE_ID, CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN, OP_CARRIER, DISTANCE "
                                       "    FROM schedule as schedule JOIN "
                                       "    (SELECT ROUTE_ID, DISTANCE "
                                       "    FROM route) as route"
                                       "      ON route.ROUTE_ID = schedule.ROUTE_ID) as schedule ON flight.SCHEDULE_ID = schedule.SCHEDULE_ID order by id;";

            begin = std::chrono::high_resolution_clock::now();
            std::cout<<update_query<<"\n";
            con.Query(update_query)->Print();
            con.Query("ALTER TABLE "+fact_table_name+"_complete_"+col_null+" ALTER COLUMN "+col_null+" SET DEFAULT 10;")->Print();//not adding b, replace s with rep
            end = std::chrono::high_resolution_clock::now();
            std::cout<<"Time updating ==1 partition (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

            //update 2 missing values
            //con.Query("SET threads TO 1;");
            update_query = "CREATE TABLE rep AS SELECT CASE WHEN "+col_null+"_IS_NULL THEN "+select_stmt+" ELSE "+col_null + " END AS new_vals FROM "
                             " ( SELECT * FROM "+fact_table_name+"_complete_2 "
                             ") as flight JOIN ("
                             "    SELECT SCHEDULE_ID, CRS_DEP_HOUR, CRS_DEP_MIN, CRS_ARR_HOUR, CRS_ARR_MIN, OP_CARRIER, DISTANCE "
                             "    FROM schedule as schedule JOIN "
                             "    (SELECT ROUTE_ID, DISTANCE "
                             "    FROM route) as route"
                             "      ON route.ROUTE_ID = schedule.ROUTE_ID) as schedule ON flight.SCHEDULE_ID = schedule.SCHEDULE_ID ORDER BY ID";


            std::cout<<update_query+"\n";
            begin = std::chrono::high_resolution_clock::now();
            con.Query(update_query)->Print();
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