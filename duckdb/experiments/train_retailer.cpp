//
// Created by Massimo Perini on 28/06/2023.
//

#include "train_retailer.h"


#include "../ML/Regression.h"
#include "../triple/helper.h"
#include <iostream>
#include <duckdb.hpp>
#include <iterator>

namespace Retailer {
    void import_data(duckdb::Connection &con, const std::string &path) {
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

        con.Query("CREATE TABLE Inventory("
                  "id INTEGER,"
                  "    locn INTEGER,"
                  "    dateid INTEGER,"
                  "    ksn INTEGER,"
                  "    inventoryunits FLOAT"
                  ");");

        con.Query("COPY Census FROM '" + path +
                  "/schedule.csv' (FORMAT CSV, AUTO_DETECT TRUE , nullstr '', DELIMITER ',')");
        con.Query("COPY Location FROM '" + path +
                  "/distances.csv' (FORMAT CSV, AUTO_DETECT TRUE , nullstr '', DELIMITER ',')");
        con.Query("COPY Item FROM '" + path +
                  "/airlines_data.csv' (FORMAT CSV, AUTO_DETECT TRUE , nullstr '', DELIMITER ',')");
        con.Query("COPY Weather FROM '" + path +
                  "/schedule.csv' (FORMAT CSV, AUTO_DETECT TRUE , nullstr '', DELIMITER ',')");
        con.Query("COPY Inventory FROM '" + path +
                  "/distances.csv' (FORMAT CSV, AUTO_DETECT TRUE , nullstr '', DELIMITER ',')");
    }


    void train_mat_sql_lift(const std::string &path, bool materialized) {
        duckdb::DuckDB db(":memory:");
        duckdb::Connection con(db);
        //import data
        import_data(con, path);

        std::vector<std::string> con_columns = {"population", "white", "asian", "pacific", "black", "medianage",
                                                "occupiedhouseunits", "houseunits", "families", "households",
                                                "husbwife", "males", "females", "householdschildren", "hispanic",
                                                "rgn_cd", "clim_zn_nbr", "tot_area_sq_ft", "sell_area_sq_ft", "avghhi",
                                                "supertargetdistance", "supertargetdrivetime", "targetdistance",
                                                "targetdrivetime", "walmartdistance", "walmartdrivetime",
                                                "walmartsupercenterdistance", "walmartsupercenterdrivetime", "prize",
                                                "feat_1", "maxtemp", "mintemp", "meanwind", "thunder",
                                                "inventoryunits"};

        if (materialized) {
            auto begin = std::chrono::high_resolution_clock::now();
            std::string aaa = "CREATE TABLE tbl_retailer AS (SELECT * FROM Weather JOIN Location USING (locn) JOIN Inventory USING (locn, dateid) JOIN Item USING (ksn) JOIN Census USING (zip))";
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

        query += "), 'lin_cat':LIST_VALUE(), 'quad_num_cat':LIST_VALUE(), 'quad_cat':LIST_VALUE()} FROM ";

        for (size_t i = 0; i < con_columns.size(); i++) {
            for (size_t j = i; j < con_columns.size(); j++) {
                query += "SUM(" + con_columns[i] + "*" + con_columns[j] + "), ";
            }
        }
        query.pop_back();
        query.pop_back();
        query += ") FROM ";
        if (materialized)
            query += "tbl_retailer";
        else
            query += "Weather JOIN Location USING (locn) JOIN Inventory USING (locn, dateid) JOIN Item USING (ksn) JOIN Census USING (zip)";

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

        std::vector<std::string> con_columns = {"population", "white", "asian", "pacific", "black", "medianage",
                                                "occupiedhouseunits", "houseunits", "families", "households",
                                                "husbwife", "males", "females", "householdschildren", "hispanic",
                                                "rgn_cd", "clim_zn_nbr", "tot_area_sq_ft", "sell_area_sq_ft", "avghhi",
                                                "supertargetdistance", "supertargetdrivetime", "targetdistance",
                                                "targetdrivetime", "walmartdistance", "walmartdrivetime",
                                                "walmartsupercenterdistance", "walmartsupercenterdrivetime", "prize",
                                                "feat_1", "maxtemp", "mintemp", "meanwind", "thunder",
                                                "inventoryunits"};
        std::vector<std::string> cat_columns = {"subcategory", "category", "categoryCluster", "rain", "snow"};

        Triple::register_functions(*con.context, {con_columns.size()}, {cat_columns.size()});

        if (materialized) {
            auto begin = std::chrono::high_resolution_clock::now();
            std::string aaa = "CREATE TABLE tbl_retailer AS (SELECT * FROM Weather JOIN Location USING (locn) JOIN Inventory USING (locn, dateid) JOIN Item USING (ksn) JOIN Census USING (zip))";
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
            query += "tbl_retailer";
        else
            query += "Weather JOIN Location USING (locn) JOIN Inventory USING (locn, dateid) JOIN Item USING (ksn) JOIN Census USING (zip)";

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

        Triple::register_functions(*con.context, {20, 4, 1, 20, 4, 1}, {3, 1, 0, 0, 0, 0});//con+cat, con only

        std::string aaa;
        //factorized
        if (categorical) {
            aaa = "SELECT "
                  "  triple_sum(multiply(t2.cnt, t3.cnt))"
                  " FROM"
                  "  ("
                  "    SELECT"
                  "      t1.locn, triple_sum(multiply("
                  "        t1.cnt, lift(W.maxtemp,"
                  "          W.mintemp, W.meanwind, W.rain, W.snow,"
                  "          W.thunder)"
                  "        )"
                  "      ) AS cnt"
                  "    FROM"
                  "      (SELECT"
                  "          Inv.locn,"
                  "          Inv.dateid,triple_sum_no_lift(Inv.inventoryunits,"
                  "              It.prize, It.subcategory,"
                  "              It.category, It.categoryCluster)"
                  "              AS cnt"
                  "        FROM"
                  "          Inventory AS Inv"
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
                  "      L.locn, triple_sum_no_lift(ARRAY[C.population,"
                  "          C.white, C.asian, C.pacific, C.black,"
                  "          C.medianage,  C.occupiedhouseunits,"
                  "          C.houseunits, C.families, C.households,C.husbwife,"
                  "          C.males, C.females, C.householdschildren,"
                  "          C.hispanic,  L.rgn_cd,"
                  "          L.clim_zn_nbr, L.tot_area_sq_ft,"
                  "          L.sell_area_sq_ft, L.avghhi, L.supertargetdistance,"
                  "          L.supertargetdrivetime, L.targetdistance,L.targetdrivetime,"
                  "          L.walmartdistance, L.walmartdrivetime,  L.walmartsupercenterdistance],ARRAY[] :: int4[]"
                  "      ) AS cnt"
                  "    FROM"
                  "      Location AS L"
                  "      JOIN Census AS C ON L.zip = C.zip"
                  "    GROUP BY"
                  "      L.locn"
                  "  ) AS t3 ON t2.locn = t3.locn;";
        } else {
            aaa = "SELECT "
                  "  triple_sum(multiply(t2.cnt , t3.cnt))"
                  " FROM"
                  "  ("
                  "    SELECT"
                  "      t1.locn, triple_sum(multiply("
                  "        t1.cnt, to_cofactor(W.maxtemp,"
                  "          W.mintemp, W.meanwind)"
                  "        )"
                  "      ) AS cnt"
                  "    FROM"
                  "      (SELECT"
                  "          Inv.locn,"
                  "          Inv.dateid,triple_sum_no_lift(Inv.inventoryunits,"
                  "              It.prize)"
                  "          AS cnt"
                  "        FROM"
                  "          Inventory AS Inv"
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
                  "      L.locn, triple_sum_no_lift(C.population,"
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
        }
        auto begin = std::chrono::high_resolution_clock::now();
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
}