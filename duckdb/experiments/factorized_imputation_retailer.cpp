//
#include <factorized_imputation_retailer.h>
#include <iostream>
#include <partition.h>
#include <triple/sub.h>
#include <triple/sum/sum.h>
#include <ML/regression.h>
#include <triple/helper.h>


static void import_data(duckdb::Connection &con, const std::string &path) {
        con.Query("CREATE TABLE Census("
                  "    zip INTEGER,"
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
                  "    hispanic FLOAT)");

        con.Query("CREATE TABLE Location("
                  "    locn INTEGER,"
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
                  "    walmartsupercenterdrivetime FLOAT);");

        con.Query("CREATE TABLE Item("
                  "    ksn INTEGER,"
                  "    subcategory INTEGER,"
                  "    category INTEGER,"
                  "    categoryCluster INTEGER,"
                  "    prize FLOAT,"
                  "    feat_1 FLOAT);");

        con.Query("CREATE TABLE Weather("
                  "    locn INTEGER,"
                  "    dateid INTEGER,"
                  "    rain INTEGER,"
                  "    snow INTEGER,"
                  "    maxtemp FLOAT,"
                  "    mintemp FLOAT,"
                  "    meanwind FLOAT,"
                  "    thunder INTEGER"
                  ");");

        con.Query("CREATE TABLE join_table("
                  "id INTEGER,"
                  "    locn INTEGER,"
                  "    dateid INTEGER,"
                  "    ksn INTEGER,"
                  "    inventoryunits FLOAT"//todo fix partitioning
                  ");");

        con.Query("COPY Census FROM '" + path +
                  "/census_dataset_postgres.csv' (FORMAT CSV, AUTO_DETECT TRUE , nullstr '', DELIMITER ',')");
        con.Query("COPY Location FROM '" + path +
                  "/location_dataset_postgres.csv' (FORMAT CSV, AUTO_DETECT TRUE , nullstr '', DELIMITER ',')");
        con.Query("COPY Item FROM '" + path +
                  "/item_dataset_postgres.csv' (FORMAT CSV, AUTO_DETECT TRUE , nullstr '', DELIMITER ',')");
        con.Query("COPY Weather FROM '" + path +
                  "/weather_dataset_postgres.csv' (FORMAT CSV, AUTO_DETECT TRUE , nullstr '', DELIMITER ',')");
        con.Query("COPY join_table FROM '" + path +
                  "/inventory_dataset_postgres_nulls.csv' (FORMAT CSV, AUTO_DETECT TRUE , nullstr '', DELIMITER ',')");
    }

//all the columns refer to fact table
    void run_flight_partition_factorized_retailer(const std::string &path, const std::string &fact_table_name,
                                                  size_t mice_iters) {


        duckdb::DuckDB db(":memory:");
        duckdb::Connection con(db);
        //import data

        import_data(con, path);

        std::vector<std::string> con_columns_fact_null = {"inventoryunits"};
        std::vector<std::string> con_columns_fact = {"id", "locn", "dateid", "ksn", "inventoryunits"};
        std::vector<std::string> cat_columns_fact_null = {};

        std::vector<std::string> con_columns_join = {"inventoryunits", "prize", "feat_1", "maxtemp",
                                                     "mintemp", "meanwind", "population", "white", "asian", "pacific",
                                                     "black", "medianage", "occupiedhouseunits",
                                                     "houseunits", "families", "households", "husbwife",
                                                     "males", "females", "householdschildren", "hispanic", "rgn_cd",
                                                     "clim_zn_nbr", "tot_area_sq_ft", "sell_area_sq_ft", "avghhi",
                                                     "supertargetdistance",
                                                     "supertargetdrivetime", "targetdistance", "targetdrivetime",
                                                     "walmartdistance", "walmartdrivetime",
                                                     "walmartsupercenterdistance", "walmartsupercenterdrivetime"};

        std::vector<std::string> cat_columns_join = {"subcategory", "category", "categoryCluster", "rain", "snow",
                                                     "thunder"};


        con.Query("ALTER TABLE " + fact_table_name + " ADD COLUMN n_nulls INTEGER DEFAULT 10;")->Print();
        std::string query = "CREATE TABLE rep AS SELECT ";

        //con_columns_fact need to be in the order of the query

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
                  " ALTER COLUMN n_nulls SET DEFAULT 10;")->Print();//not adding b, replace s with rep

        //con.Query("SELECT OP_CARRIER, COUNT(*) FROM join_table WHERE WHEELS_ON_HOUR is not null GROUP BY OP_CARRIER")->Print();
        //todo cat columns need to be overall?

        std::vector<std::pair<std::string, std::string>> cat_col_info;
        cat_col_info.push_back(std::make_pair("subcategory", "Item"));
        cat_col_info.push_back(std::make_pair("category", "Item"));
        cat_col_info.push_back(std::make_pair("categoryCluster", "Item"));
        cat_col_info.push_back(std::make_pair("rain", "Weather"));
        cat_col_info.push_back(std::make_pair("snow", "Weather"));
        cat_col_info.push_back(std::make_pair("thunder", "Weather"));
        build_list_of_uniq_categoricals(cat_col_info, con);

        //partition according to n. missing values
        auto begin = std::chrono::high_resolution_clock::now();
        partition(fact_table_name, {"inventoryunits"}, con_columns_fact_null, {"id","locn","dateid","ksn"}, {}, con, "id");
        auto end = std::chrono::high_resolution_clock::now();
        std::cout << "Time partitioning fact table (ms): "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << "\n";

    //run MICE
        //compute main cofactor
        //this is only fact table
        std::string col_select = "";
        for (auto &col: con_columns_fact) {
            col_select += col + ", ";
        }
        col_select.pop_back();
        col_select.pop_back();

        Triple::register_functions(*con.context, {3, 28}, {3, 0});//con+cat, con only

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
                "              It.prize, It.feat_1, It.subcategory,"
                "              It.category, It.categoryCluster)"
                "              AS cnt"
                "        FROM"
                "          (SELECT * from join_table_complete_0 UNION ALL SELECT * from join_table_complete_inventoryunits) AS Inv"
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
                "          L.walmartdistance, L.walmartdrivetime,  L.walmartsupercenterdistance, L.walmartsupercenterdrivetime"
                "      ) AS cnt"
                "    FROM"
                "      Location AS L"
                "      JOIN Census AS C ON L.zip = C.zip"
                "    GROUP BY"
                "      L.locn"
                "  ) AS t3 ON t2.locn = t3.locn;";


        begin = std::chrono::high_resolution_clock::now();
        duckdb::Value full_triple = con.Query(query)->GetValue(0, 0);
        end = std::chrono::high_resolution_clock::now();
        std::cout << "Time full cofactor (ms): "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << "\n";

        //start MICE

        for (int mice_iter = 0; mice_iter < mice_iters; mice_iter++) {
            //continuous cols
            for (auto &col_null: con_columns_fact_null) {
                std::cout << "\n\nColumn: " << col_null << "\n\n";
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
                                          "              It.prize, It.feat_1, It.subcategory,"
                                          "              It.category, It.categoryCluster)"
                                          "              AS cnt"
                                          "        FROM"
                                          "          (SELECT * from join_table_complete_inventoryunits) AS Inv"
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
                                          "          L.walmartdistance, L.walmartdrivetime,  L.walmartsupercenterdistance, L.walmartsupercenterdrivetime"
                                          "      ) AS cnt"
                                          "    FROM"
                                          "      Location AS L"
                                          "      JOIN Census AS C ON L.zip = C.zip"
                                          "    GROUP BY"
                                          "      L.locn"
                                          "  ) AS t3 ON t2.locn = t3.locn;";

                begin = std::chrono::high_resolution_clock::now();
                duckdb::Value null_triple = con.Query(delta_query)->GetValue(0, 0);
                end = std::chrono::high_resolution_clock::now();
                std::cout << "Time delta cofactor (ms): "
                          << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << "\n";

                duckdb::Value train_triple = Triple::subtract_triple(full_triple, null_triple);

                //train
                //auto it = std::find(con_columns_fact.begin(), con_columns_fact.end(), col_null);//is ok because fact is the first table
                //size_t label_index = it - con_columns_fact.begin();
                int label_index = 0;
                std::cout << "Label index " << label_index << "\n";
                std::vector<double> params = Triple::ridge_linear_regression(train_triple, label_index, 0.001, 0, 1000, true);
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
                new_val += cat_columns_query + "+(sqrt(-2 * ln(random()))*cos(2*pi()*random()) *"+ std::to_string(params[params.size()-1])+"))::FLOAT";


                //predict query
                //update 1 missing value
                std::cout << "CREATE TABLE rep AS SELECT " + new_val + " AS new_vals FROM ..." + col_null << "\n";


                //update 1 missing value
                std::string update_query = "CREATE TABLE rep AS SELECT " + new_val + " AS new_vals FROM"
                                                                                     "  (SELECT *"
                                                                                     "  FROM"
                                                                                     "  (SELECT * from join_table_complete_inventoryunits AS Inv"
                                                                                     "  JOIN Item AS It ON Inv.ksn = It.ksn) as t1"
                                                                                     "  JOIN Weather AS W ON t1.locn = W.locn AND t1.dateid = W.dateid ) AS t2"
                                                                                     "  JOIN ("
                                                                                     "    SELECT L.locn, C.population, C.white, C.asian, C.pacific, C.black,"
                                                                                     "          C.medianage,  C.occupiedhouseunits,"
                                                                                     "          C.houseunits, C.families, C.households,C.husbwife,"
                                                                                     "          C.males, C.females, C.householdschildren,"
                                                                                     "          C.hispanic,  L.rgn_cd,"
                                                                                     "          L.clim_zn_nbr, L.tot_area_sq_ft,"
                                                                                     "          L.sell_area_sq_ft, L.avghhi, L.supertargetdistance,"
                                                                                     "          L.supertargetdrivetime, L.targetdistance,L.targetdrivetime,"
                                                                                     "          L.walmartdistance, L.walmartdrivetime,  L.walmartsupercenterdistance, L.walmartsupercenterdrivetime"
                                                                                     "    FROM"
                                                                                     "      Location AS L"
                                                                                     "      JOIN Census AS C ON L.zip = C.zip"
                                                                                     "  ) AS t3 ON t2.locn = t3.locn ORDER BY id;";
                begin = std::chrono::high_resolution_clock::now();
                con.Query(update_query)->Print();
                con.Query("ALTER TABLE " + fact_table_name + "_complete_" + col_null + " ALTER COLUMN " + col_null +
                          " SET DEFAULT 10;")->Print();//not adding b, replace s with rep
                end = std::chrono::high_resolution_clock::now();
                std::cout << "Time updating ==1 partition (ms): "
                          << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << "\n";

                //recompute cofactor
                begin = std::chrono::high_resolution_clock::now();
                null_triple = con.Query(delta_query)->GetValue(0, 0);
                end = std::chrono::high_resolution_clock::now();
                std::cout << "Time delta cofactor (ms): "
                          << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << "\n";
                full_triple = Triple::sum_triple(train_triple, null_triple);
            }
        }
    }