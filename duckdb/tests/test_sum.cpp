//
// Created by " " on 20/06/2023.
//
#include <doctest/doctest.h>
#include <duckdb.hpp>
#include "../triple/helper.h"

#include <iostream>

#include <vector>

using namespace std;
TEST_CASE("sum") {

    duckdb::DuckDB db(":memory:");
    duckdb::Connection con(db);

    std::vector<std::string> con_columns = {"a", "b", "c"};;
    std::vector<std::string> cat_columns = {"d", "e", "f"};
    Triple::register_functions(*con.context, {con_columns.size()}, {cat_columns.size()});

    std::string ttt = "CREATE TABLE test(gb INTEGER, a FLOAT, b FLOAT, c FLOAT, d INTEGER, e INTEGER, f INTEGER);";
    con.Query(ttt);
    ttt = "INSERT INTO test VALUES (1,1,2,3,4,5,6), (1,5,6,7,8,9,10), (2,2,1,3,4,6,8), (2,5,7,6,8,10,12), (2,2,1,3,4,6,8)";
    con.Query(ttt);

    SUBCASE("sum_no_lift everything") {
        auto res = con.Query("SELECT triple_sum_no_lift(a,b,c,d,e,f) from test");
        CHECK(res->GetValue(0,0) == "{'N': 5, 'lin_agg': [15.0, 17.0, 22.0], 'quad_agg': [59.0, 71.0, 80.0, 91.0, 96.0, 112.0], 'lin_cat': [[{'key': 4, 'value': 3.0}, {'key': 8, 'value': 2.0}], [{'key': 5, 'value': 1.0}, {'key': 6, 'value': 2.0}, {'key': 9, 'value': 1.0}, {'key': 10, 'value': 1.0}], [{'key': 6, 'value': 1.0}, {'key': 8, 'value': 2.0}, {'key': 10, 'value': 1.0}, {'key': 12, 'value': 1.0}]], 'quad_num_cat': [[{'key': 4, 'value': 5.0}, {'key': 8, 'value': 10.0}], [{'key': 5, 'value': 1.0}, {'key': 6, 'value': 4.0}, {'key': 9, 'value': 5.0}, {'key': 10, 'value': 5.0}], [{'key': 6, 'value': 1.0}, {'key': 8, 'value': 4.0}, {'key': 10, 'value': 5.0}, {'key': 12, 'value': 5.0}], [{'key': 4, 'value': 4.0}, {'key': 8, 'value': 13.0}], [{'key': 5, 'value': 2.0}, {'key': 6, 'value': 2.0}, {'key': 9, 'value': 6.0}, {'key': 10, 'value': 7.0}], [{'key': 6, 'value': 2.0}, {'key': 8, 'value': 2.0}, {'key': 10, 'value': 6.0}, {'key': 12, 'value': 7.0}], [{'key': 4, 'value': 9.0}, {'key': 8, 'value': 13.0}], [{'key': 5, 'value': 3.0}, {'key': 6, 'value': 6.0}, {'key': 9, 'value': 7.0}, {'key': 10, 'value': 6.0}], [{'key': 6, 'value': 3.0}, {'key': 8, 'value': 6.0}, {'key': 10, 'value': 7.0}, {'key': 12, 'value': 6.0}]], 'quad_cat': [[{'key1': 4, 'key2': 4, 'value': 3.0}, {'key1': 8, 'key2': 8, 'value': 2.0}], [{'key1': 4, 'key2': 5, 'value': 1.0}, {'key1': 4, 'key2': 6, 'value': 2.0}, {'key1': 8, 'key2': 9, 'value': 1.0}, {'key1': 8, 'key2': 10, 'value': 1.0}], [{'key1': 4, 'key2': 6, 'value': 1.0}, {'key1': 4, 'key2': 8, 'value': 2.0}, {'key1': 8, 'key2': 10, 'value': 1.0}, {'key1': 8, 'key2': 12, 'value': 1.0}], [{'key1': 5, 'key2': 5, 'value': 1.0}, {'key1': 6, 'key2': 6, 'value': 2.0}, {'key1': 9, 'key2': 9, 'value': 1.0}, {'key1': 10, 'key2': 10, 'value': 1.0}], [{'key1': 5, 'key2': 6, 'value': 1.0}, {'key1': 6, 'key2': 8, 'value': 2.0}, {'key1': 9, 'key2': 10, 'value': 1.0}, {'key1': 10, 'key2': 12, 'value': 1.0}], [{'key1': 6, 'key2': 6, 'value': 1.0}, {'key1': 8, 'key2': 8, 'value': 2.0}, {'key1': 10, 'key2': 10, 'value': 1.0}, {'key1': 12, 'key2': 12, 'value': 1.0}]]}");
    }
    SUBCASE("sum_no_lift group by") {
        auto res = con.Query("SELECT triple_sum_no_lift(a,b,c,d,e,f) from test GROUP BY gb");
        CHECK(res->GetValue(0,0) == "{'N': 2, 'lin_agg': [6.0, 8.0, 10.0], 'quad_agg': [26.0, 32.0, 38.0, 40.0, 48.0, 58.0], 'lin_cat': [[{'key': 4, 'value': 1.0}, {'key': 8, 'value': 1.0}], [{'key': 5, 'value': 1.0}, {'key': 9, 'value': 1.0}], [{'key': 6, 'value': 1.0}, {'key': 10, 'value': 1.0}]], 'quad_num_cat': [[{'key': 4, 'value': 1.0}, {'key': 8, 'value': 5.0}], [{'key': 5, 'value': 1.0}, {'key': 9, 'value': 5.0}], [{'key': 6, 'value': 1.0}, {'key': 10, 'value': 5.0}], [{'key': 4, 'value': 2.0}, {'key': 8, 'value': 6.0}], [{'key': 5, 'value': 2.0}, {'key': 9, 'value': 6.0}], [{'key': 6, 'value': 2.0}, {'key': 10, 'value': 6.0}], [{'key': 4, 'value': 3.0}, {'key': 8, 'value': 7.0}], [{'key': 5, 'value': 3.0}, {'key': 9, 'value': 7.0}], [{'key': 6, 'value': 3.0}, {'key': 10, 'value': 7.0}]], 'quad_cat': [[{'key1': 4, 'key2': 4, 'value': 1.0}, {'key1': 8, 'key2': 8, 'value': 1.0}], [{'key1': 4, 'key2': 5, 'value': 1.0}, {'key1': 8, 'key2': 9, 'value': 1.0}], [{'key1': 4, 'key2': 6, 'value': 1.0}, {'key1': 8, 'key2': 10, 'value': 1.0}], [{'key1': 5, 'key2': 5, 'value': 1.0}, {'key1': 9, 'key2': 9, 'value': 1.0}], [{'key1': 5, 'key2': 6, 'value': 1.0}, {'key1': 9, 'key2': 10, 'value': 1.0}], [{'key1': 6, 'key2': 6, 'value': 1.0}, {'key1': 10, 'key2': 10, 'value': 1.0}]]}");
        CHECK(res->GetValue(0,1) == "{'N': 3, 'lin_agg': [9.0, 9.0, 12.0], 'quad_agg': [33.0, 39.0, 42.0, 51.0, 48.0, 54.0], 'lin_cat': [[{'key': 4, 'value': 2.0}, {'key': 8, 'value': 1.0}], [{'key': 6, 'value': 2.0}, {'key': 10, 'value': 1.0}], [{'key': 8, 'value': 2.0}, {'key': 12, 'value': 1.0}]], 'quad_num_cat': [[{'key': 4, 'value': 4.0}, {'key': 8, 'value': 5.0}], [{'key': 6, 'value': 4.0}, {'key': 10, 'value': 5.0}], [{'key': 8, 'value': 4.0}, {'key': 12, 'value': 5.0}], [{'key': 4, 'value': 2.0}, {'key': 8, 'value': 7.0}], [{'key': 6, 'value': 2.0}, {'key': 10, 'value': 7.0}], [{'key': 8, 'value': 2.0}, {'key': 12, 'value': 7.0}], [{'key': 4, 'value': 6.0}, {'key': 8, 'value': 6.0}], [{'key': 6, 'value': 6.0}, {'key': 10, 'value': 6.0}], [{'key': 8, 'value': 6.0}, {'key': 12, 'value': 6.0}]], 'quad_cat': [[{'key1': 4, 'key2': 4, 'value': 2.0}, {'key1': 8, 'key2': 8, 'value': 1.0}], [{'key1': 4, 'key2': 6, 'value': 2.0}, {'key1': 8, 'key2': 10, 'value': 1.0}], [{'key1': 4, 'key2': 8, 'value': 2.0}, {'key1': 8, 'key2': 12, 'value': 1.0}], [{'key1': 6, 'key2': 6, 'value': 2.0}, {'key1': 10, 'key2': 10, 'value': 1.0}], [{'key1': 6, 'key2': 8, 'value': 2.0}, {'key1': 10, 'key2': 12, 'value': 1.0}], [{'key1': 8, 'key2': 8, 'value': 2.0}, {'key1': 12, 'key2': 12, 'value': 1.0}]]}");
    }
    SUBCASE("sum_no_lift having") {
        auto res = con.Query("SELECT triple_sum_no_lift(a,b,c,d,e,f) from test GROUP BY gb HAVING gb = 2");
        CHECK(res->GetValue(0,0) == "{'N': 3, 'lin_agg': [9.0, 9.0, 12.0], 'quad_agg': [33.0, 39.0, 42.0, 51.0, 48.0, 54.0], 'lin_cat': [[{'key': 4, 'value': 2.0}, {'key': 8, 'value': 1.0}], [{'key': 6, 'value': 2.0}, {'key': 10, 'value': 1.0}], [{'key': 8, 'value': 2.0}, {'key': 12, 'value': 1.0}]], 'quad_num_cat': [[{'key': 4, 'value': 4.0}, {'key': 8, 'value': 5.0}], [{'key': 6, 'value': 4.0}, {'key': 10, 'value': 5.0}], [{'key': 8, 'value': 4.0}, {'key': 12, 'value': 5.0}], [{'key': 4, 'value': 2.0}, {'key': 8, 'value': 7.0}], [{'key': 6, 'value': 2.0}, {'key': 10, 'value': 7.0}], [{'key': 8, 'value': 2.0}, {'key': 12, 'value': 7.0}], [{'key': 4, 'value': 6.0}, {'key': 8, 'value': 6.0}], [{'key': 6, 'value': 6.0}, {'key': 10, 'value': 6.0}], [{'key': 8, 'value': 6.0}, {'key': 12, 'value': 6.0}]], 'quad_cat': [[{'key1': 4, 'key2': 4, 'value': 2.0}, {'key1': 8, 'key2': 8, 'value': 1.0}], [{'key1': 4, 'key2': 6, 'value': 2.0}, {'key1': 8, 'key2': 10, 'value': 1.0}], [{'key1': 4, 'key2': 8, 'value': 2.0}, {'key1': 8, 'key2': 12, 'value': 1.0}], [{'key1': 6, 'key2': 6, 'value': 2.0}, {'key1': 10, 'key2': 10, 'value': 1.0}], [{'key1': 6, 'key2': 8, 'value': 2.0}, {'key1': 10, 'key2': 12, 'value': 1.0}], [{'key1': 8, 'key2': 8, 'value': 2.0}, {'key1': 12, 'key2': 12, 'value': 1.0}]]}");
    }

    SUBCASE("sum everything") {
        auto res = con.Query("SELECT triple_sum_no_lift(a,b,c,d,e,f) from test");
        auto res1 = con.Query("SELECT triple_sum(lift(a,b,c,d,e,f)) from test");
        CHECK(res->GetValue(0,0) == res1->GetValue(0,0));
        CHECK(res->RowCount() == 1);
        CHECK(res1->RowCount() == 1);
    }
    SUBCASE("sum group by") {
        auto res = con.Query("SELECT triple_sum_no_lift(a,b,c,d,e,f) from test GROUP BY gb");
        auto res1 = con.Query("SELECT triple_sum(lift(a,b,c,d,e,f)) from test GROUP BY gb");
        CHECK(res->GetValue(0,0) == res1->GetValue(0,0));
        CHECK(res->GetValue(0,1) == res1->GetValue(0,1));
        CHECK(res->RowCount() == 2);
        CHECK(res1->RowCount() == 2);
    }
    SUBCASE("sum having") {
        auto res = con.Query("SELECT triple_sum_no_lift(a,b,c,d,e,f) from test GROUP BY gb HAVING gb = 2");
        auto res1 = con.Query("SELECT triple_sum(lift(a,b,c,d,e,f)) from test GROUP BY gb HAVING gb = 2");
        CHECK(res->GetValue(0,0) == res1->GetValue(0,0));
        CHECK(res->RowCount() == 1);
        CHECK(res1->RowCount() == 1);
    }

}

TEST_CASE("sum2") {

    duckdb::DuckDB db(":memory:");
    duckdb::Connection con(db);

    std::vector<std::string> con_columns = {"a", "b", "c"};;
    std::vector<std::string> cat_columns = {"d", "e", "f"};
    Triple::register_functions(*con.context, {con_columns.size()}, {cat_columns.size()});

    std::string ttt = "CREATE TABLE test(gb INTEGER, a FLOAT, b FLOAT, c FLOAT, d INTEGER, e INTEGER, f INTEGER);";
    con.Query(ttt);
    ttt = "INSERT INTO test VALUES (1,1,2,3,4,5,6), (1,5,6,7,8,9,10), (2,2,1,3,4,6,8), (2,5,7,6,8,10,12), (2,2,1,3,4,6,8)";//3*4
    con.Query(ttt);
    con.Query("SELECT triple_sum(lift(a,b,c,d,e,f)) from test")->Print();
    SUBCASE("sum_no_lift everything") {
        auto res = con.Query("SELECT triple_sum_no_lift(a,b,c,d,e,f) from test");
        CHECK(res->GetValue(0,0) == "{'N': 5, 'lin_agg': [15.0, 17.0, 22.0], 'quad_agg': [59.0, 71.0, 80.0, 91.0, 96.0, 112.0], 'lin_cat': [[{'key': 4, 'value': 3.0}, {'key': 8, 'value': 2.0}], [{'key': 5, 'value': 1.0}, {'key': 6, 'value': 2.0}, {'key': 9, 'value': 1.0}, {'key': 10, 'value': 1.0}], [{'key': 6, 'value': 1.0}, {'key': 8, 'value': 2.0}, {'key': 10, 'value': 1.0}, {'key': 12, 'value': 1.0}]], 'quad_num_cat': [[{'key': 4, 'value': 5.0}, {'key': 8, 'value': 10.0}], [{'key': 5, 'value': 1.0}, {'key': 6, 'value': 4.0}, {'key': 9, 'value': 5.0}, {'key': 10, 'value': 5.0}], [{'key': 6, 'value': 1.0}, {'key': 8, 'value': 4.0}, {'key': 10, 'value': 5.0}, {'key': 12, 'value': 5.0}], [{'key': 4, 'value': 4.0}, {'key': 8, 'value': 13.0}], [{'key': 5, 'value': 2.0}, {'key': 6, 'value': 2.0}, {'key': 9, 'value': 6.0}, {'key': 10, 'value': 7.0}], [{'key': 6, 'value': 2.0}, {'key': 8, 'value': 2.0}, {'key': 10, 'value': 6.0}, {'key': 12, 'value': 7.0}], [{'key': 4, 'value': 9.0}, {'key': 8, 'value': 13.0}], [{'key': 5, 'value': 3.0}, {'key': 6, 'value': 6.0}, {'key': 9, 'value': 7.0}, {'key': 10, 'value': 6.0}], [{'key': 6, 'value': 3.0}, {'key': 8, 'value': 6.0}, {'key': 10, 'value': 7.0}, {'key': 12, 'value': 6.0}]], 'quad_cat': [[{'key1': 4, 'key2': 4, 'value': 3.0}, {'key1': 8, 'key2': 8, 'value': 2.0}], [{'key1': 4, 'key2': 5, 'value': 1.0}, {'key1': 4, 'key2': 6, 'value': 2.0}, {'key1': 8, 'key2': 9, 'value': 1.0}, {'key1': 8, 'key2': 10, 'value': 1.0}], [{'key1': 4, 'key2': 6, 'value': 1.0}, {'key1': 4, 'key2': 8, 'value': 2.0}, {'key1': 8, 'key2': 10, 'value': 1.0}, {'key1': 8, 'key2': 12, 'value': 1.0}], [{'key1': 5, 'key2': 5, 'value': 1.0}, {'key1': 6, 'key2': 6, 'value': 2.0}, {'key1': 9, 'key2': 9, 'value': 1.0}, {'key1': 10, 'key2': 10, 'value': 1.0}], [{'key1': 5, 'key2': 6, 'value': 1.0}, {'key1': 6, 'key2': 8, 'value': 2.0}, {'key1': 9, 'key2': 10, 'value': 1.0}, {'key1': 10, 'key2': 12, 'value': 1.0}], [{'key1': 6, 'key2': 6, 'value': 1.0}, {'key1': 8, 'key2': 8, 'value': 2.0}, {'key1': 10, 'key2': 10, 'value': 1.0}, {'key1': 12, 'key2': 12, 'value': 1.0}]]}");
    }
}
