//
// Created by Massimo Perini on 14/06/2023.
//

#include "partition.h"
#include <iostream>
#include <string>
#include <iterator>


void partition(const std::string &table_name, const std::vector<std::string> &con_columns, const std::vector<std::string> &con_columns_nulls,
               const std::vector<std::string> &cat_columns, const std::vector<std::string> &cat_columns_nulls, duckdb::Connection &con){

    //query averages (SELECT AVG(col) FROM table LIMIT 10000)
    std::string query = "SELECT ";
    for (auto &col: con_columns_nulls)
        query += "AVG("+col+"), ";
    for (auto &col: cat_columns_nulls)
        query += "MODE("+col+"), ";
    query.pop_back();
    query.pop_back();
    query += " FROM "+table_name+" LIMIT 10000";
    auto collection = con.Query(query);
    std::vector<float> avg = {};
    for (idx_t col_index = 0; col_index<con_columns_nulls.size()+cat_columns_nulls.size(); col_index++){
        duckdb::Value v = collection->GetValue(col_index, 0);
        avg.push_back(v.GetValue<float>());
    }

    //CREATE NEW TABLES
    //0, _col_name, 2
    //con.Query("SELECT * from join_table WHERE n_nulls = 0 LIMIT 50")->Print();
    std::string create_table_query = "CREATE TABLE "+table_name+"_complete_0(";
    std::string insert_query = "INSERT INTO "+table_name+"_complete_0 SELECT ";

    for (auto &col: con_columns){
        create_table_query += col+" FLOAT, ";
        insert_query += col+", ";
    }
    for (auto &col: cat_columns){
        create_table_query += col+" INTEGER, ";
        insert_query += col+", ";
    }
    create_table_query.pop_back();
    create_table_query.pop_back();
    insert_query.pop_back();
    insert_query.pop_back();
    insert_query += " FROM "+table_name+" WHERE n_nulls = 0";
    con.Query(create_table_query+")");
    con.Query(insert_query);

    //create tables 1 missing value
    idx_t col_index = 0;
    for(auto &col_null: con_columns_nulls){
        create_table_query = "CREATE TABLE "+table_name+"_complete_"+col_null+"(";
        insert_query = "INSERT INTO "+table_name+"_complete_"+col_null+" SELECT ";

        for (auto &col: con_columns){
            create_table_query += col+" FLOAT, ";
            if (col == col_null)
                insert_query += "COALESCE ("+col+", "+std::to_string(avg[col_index])+"), ";
            else
                insert_query += col+", ";
        }
        for (auto &col: cat_columns){
            create_table_query += col+" INTEGER, ";
            insert_query += col+", ";
        }
        create_table_query.pop_back();
        create_table_query.pop_back();
        insert_query.pop_back();
        insert_query.pop_back();
        con.Query(create_table_query+")");
        con.Query(insert_query+" FROM "+table_name+" WHERE n_nulls = 1 AND "+col_null+" IS NULL");
        //con.Query("SELECT COUNT(*) FROM "+table_name+"_complete_"+col_null);
        col_index ++;
    }

    for(auto &col_null: cat_columns_nulls){
        create_table_query = "CREATE TABLE "+table_name+"_complete_"+col_null+"(";
        insert_query = "INSERT INTO "+table_name+"_complete_"+col_null+" SELECT ";

        for (auto &col: con_columns){
            create_table_query += col+" FLOAT, ";
            insert_query += col+", ";
        }
        for (auto &col: cat_columns){
            create_table_query += col+" INTEGER, ";
            if (col == col_null)
                insert_query += "COALESCE ("+col+", "+std::to_string(avg[col_index])+"), ";
            else
                insert_query += col+", ";
        }
        create_table_query.pop_back();
        create_table_query.pop_back();
        insert_query.pop_back();
        insert_query.pop_back();
        con.Query(create_table_query+")");
        con.Query(insert_query+" FROM "+table_name+" WHERE n_nulls = 1 AND "+col_null+" IS NULL");
        col_index ++;
    }


    //create tables >=2 missing values
    create_table_query = "CREATE TABLE "+table_name+"_complete_2(";
    insert_query = "INSERT INTO "+table_name+"_complete_2 SELECT ";
    for (auto &col: con_columns){
        create_table_query += col+" FLOAT, ";
        auto null = std::find(con_columns_nulls.begin(), con_columns_nulls.end(), col);
        if (null != con_columns_nulls.end()) {
            //this column contains null
            create_table_query += col+"_IS_NULL BOOL, ";
            insert_query += "COALESCE (" + col + ", " + std::to_string(avg[null - con_columns_nulls.begin()]) + "), "+col+" IS NULL, ";
        }
        else
            insert_query += col+", ";
    }

    for (auto &col: cat_columns){
        create_table_query += col+" INTEGER, ";
        auto null = std::find(cat_columns_nulls.begin(), cat_columns_nulls.end(), col);
        if (null != cat_columns_nulls.end()) {
            create_table_query += col+"_IS_NULL BOOL, ";
            insert_query += "COALESCE (" + col + ", " + std::to_string(avg[null - cat_columns_nulls.begin()+con_columns_nulls.size()]) + "), "+col+" IS NULL, ";
        }
        else
            insert_query += col+", ";
    }
    create_table_query.pop_back();
    create_table_query.pop_back();
    insert_query.pop_back();
    insert_query.pop_back();

    con.Query(create_table_query+")");
    con.Query(insert_query+" FROM "+table_name+" WHERE n_nulls >= 2");

    //table is partitioned
}

void init_baseline(const std::string &table_name, const std::vector<std::string> &con_columns_nulls, const std::vector<std::string> &cat_columns_nulls, duckdb::Connection &con){
    std::string query = "SELECT ";
    for (auto &col: con_columns_nulls)
        query += "AVG("+col+"), ";
    for (auto &col: cat_columns_nulls)
        query += "MODE("+col+"), ";
    query.pop_back();
    query.pop_back();
    query += " FROM "+table_name+" LIMIT 10000";
    auto collection = con.Query(query);
    con.Query("CREATE TABLE "+table_name+"_complete AS SELECT * FROM "+table_name);
    std::vector<float> avg = {};
    for (idx_t col_index = 0; col_index<con_columns_nulls.size()+cat_columns_nulls.size(); col_index++){
        duckdb::Value v = collection->GetValue(col_index, 0);
        avg.push_back(v.GetValue<float>());
    }

    for (idx_t col_index = 0; col_index<con_columns_nulls.size()+cat_columns_nulls.size(); col_index++){
        duckdb::Value v = collection->GetValue(col_index, 0);
        avg.push_back(v.GetValue<float>());
    }

    for (idx_t col_index = 0; col_index<con_columns_nulls.size(); col_index++){
        float rep = avg[col_index];
        std::string query = "CREATE TABLE rep AS SELECT "+con_columns_nulls[col_index]+" IS NULL FROM "+table_name;
        con.Query(query);
        con.Query("ALTER TABLE "+table_name+"_complete ADD COLUMN "+con_columns_nulls[col_index]+"_IS_NULL BOOLEAN DEFAULT false;")->Print();
        con.Query("ALTER TABLE "+table_name+"_complete ALTER COLUMN "+con_columns_nulls[col_index]+"_IS_NULL SET DEFAULT 10;")->Print();//not adding b, replace s with rep

        query = "CREATE TABLE rep AS SELECT COALESCE("+con_columns_nulls[col_index]+" , "+ std::to_string(rep) +") FROM "+table_name;
        con.Query(query);
        con.Query("ALTER TABLE "+table_name+"_complete ALTER COLUMN "+con_columns_nulls[col_index]+" SET DEFAULT 10;")->Print();//not adding b, replace s with rep
    }

    for (idx_t col_index = 0; col_index<cat_columns_nulls.size(); col_index++){
        int rep = (int) avg[col_index+con_columns_nulls.size()];
        std::string query = "CREATE TABLE rep AS SELECT "+cat_columns_nulls[col_index]+" IS NULL FROM "+table_name;
        con.Query(query);
        con.Query("ALTER TABLE "+table_name+"_complete ADD COLUMN "+cat_columns_nulls[col_index]+"_IS_NULL BOOLEAN DEFAULT false;")->Print();
        con.Query("ALTER TABLE "+table_name+"_complete ALTER COLUMN "+cat_columns_nulls[col_index]+"_IS_NULL SET DEFAULT 10;")->Print();//not adding b, replace s with rep

        query = "CREATE TABLE rep AS SELECT COALESCE("+cat_columns_nulls[col_index]+" , "+ std::to_string(rep) +") FROM "+table_name;
        con.Query(query);
        con.Query("ALTER TABLE "+table_name+"_complete ALTER COLUMN "+cat_columns_nulls[col_index]+" SET DEFAULT 10;")->Print();//not adding b, replace s with rep
    }

}

namespace {
    std::vector<std::vector<int>> uniq_cat_vals = {};
}

void build_list_of_uniq_categoricals(const std::vector<std::string> &cat_columns, duckdb::Connection &con, const std::string &table_name){
    std::string ttt;
    for (size_t i=0; i<cat_columns.size(); i++) {
        ttt = "SELECT DISTINCT "+cat_columns[i]+" from "+table_name+" WHERE "+cat_columns[i]+" IS NOT NULL ORDER BY "+cat_columns[i];
        auto uniq_vals = con.Query(ttt);
        uniq_cat_vals.emplace_back();
        for(size_t j=0; j<uniq_vals->RowCount(); j++){
            uniq_cat_vals[i].push_back(uniq_vals->GetValue(0, j).GetValue<int>());
        }
    }
}

void query_categorical(const std::vector<std::string> &cat_columns, size_t label, std::string &cat_columns_query, std::string &predict_column_query){
    //store unique vals

    for (size_t i=0; i<cat_columns.size(); i++) {
        auto &col_items = uniq_cat_vals[i];
        std::stringstream  s;
        std::string delimiter = ", ";
        copy(col_items.begin(),col_items.end(), std::ostream_iterator<int>(s,delimiter.c_str()));
        auto list = s.str();
        list.pop_back();
        list.pop_back();
        if (i == label) {
            predict_column_query = "["+list+"]";
        }
        cat_columns_query += "list_position(["+list+"], "+cat_columns[i]+")-1, ";
    }
    cat_columns_query.pop_back();
    cat_columns_query.pop_back();
}