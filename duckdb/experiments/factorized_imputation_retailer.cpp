//
// Created by Massimo Perini on 11/07/2023.
//

#include "factorized_imputation_retailer.h"
#include <iostream>
#include "../partition.h"
#include "../triple/Triple_sub.h"
#include "../triple/Triple_sum.h"
#include "../ML/lda.h"
#include "../ML/Regression.h"
#include "../triple/helper.h"

//all the columns refer to fact table
void run_flight_partition_factorized_retailer(duckdb::Connection &con, const std::vector<std::string> &con_columns_fact, const std::vector<std::string> &cat_columns, const std::vector<std::string> &con_columns_nulls, const std::vector<std::string> &cat_columns_nulls, const std::string &fact_table_name, size_t mice_iters){
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
    //todo cat columns need to be overall?
    build_list_of_uniq_categoricals(cat_columns, con, fact_table_name);
    //partition according to n. missing values
    auto begin = std::chrono::high_resolution_clock::now();
    std::vector<std::string> vals_partition = std::vector(con_columns_fact);
    vals_partition.push_back("id");
    partition(fact_table_name, con_columns_fact, con_columns_nulls, cat_columns, cat_columns_nulls, con, "id");
    auto end = std::chrono::high_resolution_clock::now();
    std::cout<<"Time partitioning fact table (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

    //run MICE
    //compute main cofactor
    //this is only fact table
    std::string col_select = "";
    for(auto &col: con_columns_fact){
        col_select +=col+", ";
    }
    for(auto &col: cat_columns){
        col_select +=col+", ";
    }
    col_select.pop_back();
    col_select.pop_back();

    Triple::register_functions(*con.context, {2, 27}, {3, 0});//con+cat, con only

    query = "SELECT "
          "  triple_sum(multiply_triple(t2.cnt, t3.cnt))"
          " FROM"
          "  ("
          "    SELECT"
          "      t1.locn, triple_sum(multiply_triple("
          "        t1.cnt, lift(W.maxtemp,"
          "          W.mintemp, W.meanwind, W.rain, W.snow,"
          "          W.thunder)"
          "        )"
          "      ) AS cnt"
          "    FROM"
          "      (SELECT"
          "          Inv.locn,"
          "          Inv.dateid,triple_sum_no_lift1(Inv.inventoryunits,"
          "              It.prize, It.subcategory,"
          "              It.category, It.categoryCluster)"
          "              AS cnt"
          "        FROM"
          "          (SELECT "+col_select+" from join_table_complete_0 UNION ALL SELECT "+col_select+" from join_table_complete_inventoryunits)) AS Inv"
          "          JOIN Item AS It ON Inv.ksn = It.ksn"
          "        GROUP BY"
          "          Inv.locn,"
          "          Inv.dateid"
          "      ) AS t1"
          "      JOIN Weather AS W ON t1.locn = W.locn"
          "      AND t1.dateid = W.dateid"
          "    GROUP BY"
          "      t1.locn"
          "  ) AS t2"
          "  JOIN ("
          "    SELECT"
          "      L.locn, triple_sum_no_lift2(C.population,"
          "          C.white, C.asian, C.pacific, C.black,"
          "          C.medianage,  C.occupiedhouseunits,"
          "          C.houseunits, C.families, C.households,C.husbwife,"
          "          C.males, C.females, C.householdschildren,"
          "          C.hispanic,  L.rgn_cd,"
          "          L.clim_zn_nbr, L.tot_area_sq_ft,"
          "          L.sell_area_sq_ft, L.avghhi, L.supertargetdistance,"
          "          L.supertargetdrivetime, L.targetdistance,L.targetdrivetime,"
          "          L.walmartdistance, L.walmartdrivetime,  L.walmartsupercenterdistance"
          "      ) AS cnt"
          "    FROM"
          "      Location AS L"
          "      JOIN Census AS C ON L.zip = C.zip"
          "    GROUP BY"
          "      L.locn"
          "  ) AS t3 ON t2.locn = t3.locn;";


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
            std::string delta_query = "SELECT "
                    "  triple_sum(multiply_triple(t2.cnt, t3.cnt))"
                    " FROM"
                    "  ("
                    "    SELECT"
                    "      t1.locn, triple_sum(multiply_triple("
                    "        t1.cnt, lift(W.maxtemp,"
                    "          W.mintemp, W.meanwind, W.rain, W.snow,"
                    "          W.thunder)"
                    "        )"
                    "      ) AS cnt"
                    "    FROM"
                    "      (SELECT"
                    "          Inv.locn,"
                    "          Inv.dateid,triple_sum_no_lift1(Inv.inventoryunits,"
                    "              It.prize, It.subcategory,"
                    "              It.category, It.categoryCluster)"
                    "              AS cnt"
                    "        FROM"
                    "          (SELECT "+col_select+" from join_table_complete_inventoryunits)) AS Inv"
                    "          JOIN Item AS It ON Inv.ksn = It.ksn"
                    "        GROUP BY"
                    "          Inv.locn,"
                    "          Inv.dateid"
                    "      ) AS t1"
                    "      JOIN Weather AS W ON t1.locn = W.locn"
                    "      AND t1.dateid = W.dateid"
                    "    GROUP BY"
                    "      t1.locn"
                    "  ) AS t2"
                    "  JOIN ("
                    "    SELECT"
                    "      L.locn, triple_sum_no_lift2(C.population,"
                    "          C.white, C.asian, C.pacific, C.black,"
                    "          C.medianage,  C.occupiedhouseunits,"
                    "          C.houseunits, C.families, C.households,C.husbwife,"
                    "          C.males, C.females, C.householdschildren,"
                    "          C.hispanic,  L.rgn_cd,"
                    "          L.clim_zn_nbr, L.tot_area_sq_ft,"
                    "          L.sell_area_sq_ft, L.avghhi, L.supertargetdistance,"
                    "          L.supertargetdrivetime, L.targetdistance,L.targetdrivetime,"
                    "          L.walmartdistance, L.walmartdrivetime,  L.walmartsupercenterdistance"
                    "      ) AS cnt"
                    "    FROM"
                    "      Location AS L"
                    "      JOIN Census AS C ON L.zip = C.zip"
                    "    GROUP BY"
                    "      L.locn"
                    "  ) AS t3 ON t2.locn = t3.locn;";

            begin = std::chrono::high_resolution_clock::now();
            duckdb::Value null_triple = con.Query(delta_query)->GetValue(0,0);
            end = std::chrono::high_resolution_clock::now();
            std::cout<<"Time delta cofactor (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

            duckdb::Value train_triple = Triple::subtract_triple(full_triple, null_triple);

            //train
            //auto it = std::find(con_columns_fact.begin(), con_columns_fact.end(), col_null);//is ok because fact is the first table
            //size_t label_index = it - con_columns_fact.begin();
            int label_index = 0;
            std::cout<<"Label index "<<label_index<<"\n";

            auto train_params =  lda_train(train_triple, label_index, 0.4);
            //predict query

            std::vector<std::string> con_columns_full = {"DEP_DELAY", "TAXI_OUT", "TAXI_IN", "ARR_DELAY", "ACTUAL_ELAPSED_TIME",
                                                         "AIR_TIME", "DEP_TIME_HOUR", "DEP_TIME_MIN", "WHEELS_OFF_HOUR", "WHEELS_OFF_MIN", "WHEELS_ON_HOUR", "WHEELS_ON_MIN",  "ARR_TIME_HOUR",
                                                         "ARR_TIME_MIN", "MONTH_SIN", "MONTH_COS", "DAY_SIN", "DAY_COS", "WEEKDAY_SIN", "WEEKDAY_COS",
                                                         "CRS_DEP_HOUR", "CRS_DEP_MIN", "CRS_ARR_HOUR", "CRS_ARR_MIN", "DISTANCE"};

            std::vector<std::string> cat_columns_full = {"DIVERTED", "EXTRA_DAY_ARR", "EXTRA_DAY_DEP", "OP_CARRIER"};

            //todo need to be all columns
            std::string cat_columns_query;
            std::string predict_column_query;
            query_categorical(cat_columns, label_index, cat_columns_query, predict_column_query);
            std::string delimiter = ", ";
            std::stringstream  s;
            copy(con_columns_fact.begin(),con_columns_fact.end(), std::ostream_iterator<std::string>(s,delimiter.c_str()));
            std::string num_cols_query = s.str();
            num_cols_query.pop_back();
            num_cols_query.pop_back();
            std::string select_stmt = (" list_extract("+predict_column_query+", predict_lda("+train_params.ToString()+"::FLOAT[], "+num_cols_query+", "+cat_columns_query+")+1)");

            //predict query
            //update 1 missing value
            std::cout<<"CREATE TABLE rep AS SELECT "+select_stmt+" AS new_vals FROM join_table_complete_"+col_null<<"\n";


            //update 1 missing value
            std::string update_query = "CREATE TABLE rep AS SELECT "+select_stmt+" AS new_vals FROM"
            "  (SELECT id, Inv.locn, It.prize, It.subcategory, It.category, It.categoryCluster"
            "  FROM"
                "  (SELECT * from join_table_complete_inventoryunits AS Inv"
                "  JOIN Item AS It ON Inv.ksn = It.ksn) AS t1"
            "  JOIN Weather AS W ON t1.locn = W.locn AND t1.dateid = W.dateid ) AS t2"

            "  JOIN ("
            "    SELECT C.population, C.white, C.asian, C.pacific, C.black,"
                                            "          C.medianage,  C.occupiedhouseunits,"
                                            "          C.houseunits, C.families, C.households,C.husbwife,"
                                            "          C.males, C.females, C.householdschildren,"
                                            "          C.hispanic,  L.rgn_cd,"
                                            "          L.clim_zn_nbr, L.tot_area_sq_ft,"
                                            "          L.sell_area_sq_ft, L.avghhi, L.supertargetdistance,"
                                            "          L.supertargetdrivetime, L.targetdistance,L.targetdrivetime,"
                                            "          L.walmartdistance, L.walmartdrivetime,  L.walmartsupercenterdistance"
                                            "    FROM"
                                            "      Location AS L"
                                            "      JOIN Census AS C ON L.zip = C.zip"
                                            "  ) AS t3 ON t2.locn = t3.locn ORDER BY id;";
            begin = std::chrono::high_resolution_clock::now();
            con.Query(update_query);
            con.Query("ALTER TABLE "+fact_table_name+"_complete_"+col_null+" ALTER COLUMN "+col_null+" SET DEFAULT 10;")->Print();//not adding b, replace s with rep
            end = std::chrono::high_resolution_clock::now();
            std::cout<<"Time updating ==1 partition (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

            //recompute cofactor
            begin = std::chrono::high_resolution_clock::now();
            null_triple = con.Query(delta_query)->GetValue(0,0);
            end = std::chrono::high_resolution_clock::now();
            std::cout<<"Time delta cofactor (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";
            full_triple = Triple::sum_triple(train_triple, null_triple);
        }
    }
}