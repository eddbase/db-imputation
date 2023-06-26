//
// Created by Massimo Perini on 20/06/2023.
//

#include <doctest/doctest.h>
#include <duckdb.hpp>
#include "../triple/helper.h"

#include <iostream>

#include <vector>

using namespace std;
TEST_CASE("lift") {

    duckdb::DuckDB db(":memory:");
    duckdb::Connection con(db);

    std::vector<std::string> con_columns = {"a", "b", "c"};;
    std::vector<std::string> cat_columns = {"d", "e", "f"};
    Triple::register_functions(*con.context, {con_columns.size()}, {cat_columns.size()});

    std::string ttt = "CREATE TABLE test(gb INTEGER, a FLOAT, b FLOAT, c FLOAT, d INTEGER, e INTEGER, f INTEGER);";
    con.Query(ttt);
    ttt = "INSERT INTO test VALUES (1,1,2,3,4,5,6), (1,5,6,7,8,9,10), (2,2,1,3,4,6,8), (1,5,7,6,8,10,12), (2,2,1,3,4,6,8)";
    con.Query(ttt);

    SUBCASE("lift all attributes") {
        auto res = con.Query("SELECT lift(a,b,c,d,e,f) from test");
        CHECK(res->GetValue(0,0).ToString() == "{'N': 1, 'lin_num': [1.0, 2.0, 3.0], 'quad_num': [1.0, 2.0, 3.0, 4.0, 6.0, 9.0], 'lin_cat': [[{'key': 4, 'value': 1.0}], [{'key': 5, 'value': 1.0}], [{'key': 6, 'value': 1.0}]], 'quad_num_cat': [[{'key': 4, 'value': 1.0}], [{'key': 5, 'value': 1.0}], [{'key': 6, 'value': 1.0}], [{'key': 4, 'value': 2.0}], [{'key': 5, 'value': 2.0}], [{'key': 6, 'value': 2.0}], [{'key': 4, 'value': 3.0}], [{'key': 5, 'value': 3.0}], [{'key': 6, 'value': 3.0}]], 'quad_cat': [[{'key1': 4, 'key2': 4, 'value': 1.0}], [{'key1': 4, 'key2': 5, 'value': 1.0}], [{'key1': 4, 'key2': 6, 'value': 1.0}], [{'key1': 5, 'key2': 5, 'value': 1.0}], [{'key1': 5, 'key2': 6, 'value': 1.0}], [{'key1': 6, 'key2': 6, 'value': 1.0}]]}");
        CHECK(res->GetValue(0,1).ToString() == "{'N': 1, 'lin_num': [5.0, 6.0, 7.0], 'quad_num': [25.0, 30.0, 35.0, 36.0, 42.0, 49.0], 'lin_cat': [[{'key': 8, 'value': 1.0}], [{'key': 9, 'value': 1.0}], [{'key': 10, 'value': 1.0}]], 'quad_num_cat': [[{'key': 8, 'value': 5.0}], [{'key': 9, 'value': 5.0}], [{'key': 10, 'value': 5.0}], [{'key': 8, 'value': 6.0}], [{'key': 9, 'value': 6.0}], [{'key': 10, 'value': 6.0}], [{'key': 8, 'value': 7.0}], [{'key': 9, 'value': 7.0}], [{'key': 10, 'value': 7.0}]], 'quad_cat': [[{'key1': 8, 'key2': 8, 'value': 1.0}], [{'key1': 8, 'key2': 9, 'value': 1.0}], [{'key1': 8, 'key2': 10, 'value': 1.0}], [{'key1': 9, 'key2': 9, 'value': 1.0}], [{'key1': 9, 'key2': 10, 'value': 1.0}], [{'key1': 10, 'key2': 10, 'value': 1.0}]]}");
        CHECK(res->GetValue(0,2).ToString() == "{'N': 1, 'lin_num': [2.0, 1.0, 3.0], 'quad_num': [4.0, 2.0, 6.0, 1.0, 3.0, 9.0], 'lin_cat': [[{'key': 4, 'value': 1.0}], [{'key': 6, 'value': 1.0}], [{'key': 8, 'value': 1.0}]], 'quad_num_cat': [[{'key': 4, 'value': 2.0}], [{'key': 6, 'value': 2.0}], [{'key': 8, 'value': 2.0}], [{'key': 4, 'value': 1.0}], [{'key': 6, 'value': 1.0}], [{'key': 8, 'value': 1.0}], [{'key': 4, 'value': 3.0}], [{'key': 6, 'value': 3.0}], [{'key': 8, 'value': 3.0}]], 'quad_cat': [[{'key1': 4, 'key2': 4, 'value': 1.0}], [{'key1': 4, 'key2': 6, 'value': 1.0}], [{'key1': 4, 'key2': 8, 'value': 1.0}], [{'key1': 6, 'key2': 6, 'value': 1.0}], [{'key1': 6, 'key2': 8, 'value': 1.0}], [{'key1': 8, 'key2': 8, 'value': 1.0}]]}");
        CHECK(res->GetValue(0,3).ToString() == "{'N': 1, 'lin_num': [5.0, 7.0, 6.0], 'quad_num': [25.0, 35.0, 30.0, 49.0, 42.0, 36.0], 'lin_cat': [[{'key': 8, 'value': 1.0}], [{'key': 10, 'value': 1.0}], [{'key': 12, 'value': 1.0}]], 'quad_num_cat': [[{'key': 8, 'value': 5.0}], [{'key': 10, 'value': 5.0}], [{'key': 12, 'value': 5.0}], [{'key': 8, 'value': 7.0}], [{'key': 10, 'value': 7.0}], [{'key': 12, 'value': 7.0}], [{'key': 8, 'value': 6.0}], [{'key': 10, 'value': 6.0}], [{'key': 12, 'value': 6.0}]], 'quad_cat': [[{'key1': 8, 'key2': 8, 'value': 1.0}], [{'key1': 8, 'key2': 10, 'value': 1.0}], [{'key1': 8, 'key2': 12, 'value': 1.0}], [{'key1': 10, 'key2': 10, 'value': 1.0}], [{'key1': 10, 'key2': 12, 'value': 1.0}], [{'key1': 12, 'key2': 12, 'value': 1.0}]]}");
        CHECK(res->GetValue(0,4).ToString() == "{'N': 1, 'lin_num': [2.0, 1.0, 3.0], 'quad_num': [4.0, 2.0, 6.0, 1.0, 3.0, 9.0], 'lin_cat': [[{'key': 4, 'value': 1.0}], [{'key': 6, 'value': 1.0}], [{'key': 8, 'value': 1.0}]], 'quad_num_cat': [[{'key': 4, 'value': 2.0}], [{'key': 6, 'value': 2.0}], [{'key': 8, 'value': 2.0}], [{'key': 4, 'value': 1.0}], [{'key': 6, 'value': 1.0}], [{'key': 8, 'value': 1.0}], [{'key': 4, 'value': 3.0}], [{'key': 6, 'value': 3.0}], [{'key': 8, 'value': 3.0}]], 'quad_cat': [[{'key1': 4, 'key2': 4, 'value': 1.0}], [{'key1': 4, 'key2': 6, 'value': 1.0}], [{'key1': 4, 'key2': 8, 'value': 1.0}], [{'key1': 6, 'key2': 6, 'value': 1.0}], [{'key1': 6, 'key2': 8, 'value': 1.0}], [{'key1': 8, 'key2': 8, 'value': 1.0}]]}");
    }
    SUBCASE("lift single numerical") {
        auto res = con.Query("SELECT lift(e) from test");
        CHECK(res->GetValue(0,0).ToString() == "{'N': 1, 'lin_num': [], 'quad_num': [], 'lin_cat': [[{'key': 5, 'value': 1.0}]], 'quad_num_cat': [], 'quad_cat': [[{'key1': 5, 'key2': 5, 'value': 1.0}]]}");
        CHECK(res->GetValue(0,1).ToString() == "{'N': 1, 'lin_num': [], 'quad_num': [], 'lin_cat': [[{'key': 9, 'value': 1.0}]], 'quad_num_cat': [], 'quad_cat': [[{'key1': 9, 'key2': 9, 'value': 1.0}]]}");
        CHECK(res->GetValue(0,2).ToString() == "{'N': 1, 'lin_num': [], 'quad_num': [], 'lin_cat': [[{'key': 6, 'value': 1.0}]], 'quad_num_cat': [], 'quad_cat': [[{'key1': 6, 'key2': 6, 'value': 1.0}]]}");
        CHECK(res->GetValue(0,3).ToString() == "{'N': 1, 'lin_num': [], 'quad_num': [], 'lin_cat': [[{'key': 10, 'value': 1.0}]], 'quad_num_cat': [], 'quad_cat': [[{'key1': 10, 'key2': 10, 'value': 1.0}]]}");
        CHECK(res->GetValue(0,4).ToString() == "{'N': 1, 'lin_num': [], 'quad_num': [], 'lin_cat': [[{'key': 6, 'value': 1.0}]], 'quad_num_cat': [], 'quad_cat': [[{'key1': 6, 'key2': 6, 'value': 1.0}]]}");
    }
    SUBCASE("lift single categorical") {
        auto res = con.Query("SELECT lift(a) from test");
        CHECK(res->GetValue(0,0).ToString() == "{'N': 1, 'lin_num': [1.0], 'quad_num': [1.0], 'lin_cat': [], 'quad_num_cat': [], 'quad_cat': []}");
        CHECK(res->GetValue(0,1).ToString() == "{'N': 1, 'lin_num': [5.0], 'quad_num': [25.0], 'lin_cat': [], 'quad_num_cat': [], 'quad_cat': []}");
        CHECK(res->GetValue(0,2).ToString() == "{'N': 1, 'lin_num': [2.0], 'quad_num': [4.0], 'lin_cat': [], 'quad_num_cat': [], 'quad_cat': []}");
        CHECK(res->GetValue(0,3).ToString() == "{'N': 1, 'lin_num': [5.0], 'quad_num': [25.0], 'lin_cat': [], 'quad_num_cat': [], 'quad_cat': []}");
        CHECK(res->GetValue(0,4).ToString() == "{'N': 1, 'lin_num': [2.0], 'quad_num': [4.0], 'lin_cat': [], 'quad_num_cat': [], 'quad_cat': []}");
    }
    SUBCASE("Lift with where") {
        auto res = con.Query("SELECT lift(a,b,c,d,e,f) from test where gb = 2");
        CHECK(res->GetValue(0,0).ToString() == "{'N': 1, 'lin_num': [2.0, 1.0, 3.0], 'quad_num': [4.0, 2.0, 6.0, 1.0, 3.0, 9.0], 'lin_cat': [[{'key': 4, 'value': 1.0}], [{'key': 6, 'value': 1.0}], [{'key': 8, 'value': 1.0}]], 'quad_num_cat': [[{'key': 4, 'value': 2.0}], [{'key': 6, 'value': 2.0}], [{'key': 8, 'value': 2.0}], [{'key': 4, 'value': 1.0}], [{'key': 6, 'value': 1.0}], [{'key': 8, 'value': 1.0}], [{'key': 4, 'value': 3.0}], [{'key': 6, 'value': 3.0}], [{'key': 8, 'value': 3.0}]], 'quad_cat': [[{'key1': 4, 'key2': 4, 'value': 1.0}], [{'key1': 4, 'key2': 6, 'value': 1.0}], [{'key1': 4, 'key2': 8, 'value': 1.0}], [{'key1': 6, 'key2': 6, 'value': 1.0}], [{'key1': 6, 'key2': 8, 'value': 1.0}], [{'key1': 8, 'key2': 8, 'value': 1.0}]]}");
        CHECK(res->GetValue(0,1).ToString() == "{'N': 1, 'lin_num': [2.0, 1.0, 3.0], 'quad_num': [4.0, 2.0, 6.0, 1.0, 3.0, 9.0], 'lin_cat': [[{'key': 4, 'value': 1.0}], [{'key': 6, 'value': 1.0}], [{'key': 8, 'value': 1.0}]], 'quad_num_cat': [[{'key': 4, 'value': 2.0}], [{'key': 6, 'value': 2.0}], [{'key': 8, 'value': 2.0}], [{'key': 4, 'value': 1.0}], [{'key': 6, 'value': 1.0}], [{'key': 8, 'value': 1.0}], [{'key': 4, 'value': 3.0}], [{'key': 6, 'value': 3.0}], [{'key': 8, 'value': 3.0}]], 'quad_cat': [[{'key1': 4, 'key2': 4, 'value': 1.0}], [{'key1': 4, 'key2': 6, 'value': 1.0}], [{'key1': 4, 'key2': 8, 'value': 1.0}], [{'key1': 6, 'key2': 6, 'value': 1.0}], [{'key1': 6, 'key2': 8, 'value': 1.0}], [{'key1': 8, 'key2': 8, 'value': 1.0}]]}");
        CHECK(res->RowCount() == 2);
    }
    SUBCASE("Lift with sum of columns") {
        auto res = con.Query("SELECT lift(a+b+c) from test");
        CHECK(res->GetValue(0,0).ToString() == "{'N': 1, 'lin_num': [6.0], 'quad_num': [36.0], 'lin_cat': [], 'quad_num_cat': [], 'quad_cat': []}");
        CHECK(res->GetValue(0,1).ToString() == "{'N': 1, 'lin_num': [18.0], 'quad_num': [324.0], 'lin_cat': [], 'quad_num_cat': [], 'quad_cat': []}");
        CHECK(res->GetValue(0,2).ToString() == "{'N': 1, 'lin_num': [6.0], 'quad_num': [36.0], 'lin_cat': [], 'quad_num_cat': [], 'quad_cat': []}");
    }
}
