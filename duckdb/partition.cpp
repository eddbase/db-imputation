//
// Created by Massimo Perini on 14/06/2023.
//

#include "partition.h"
#include <iostream>
#include <string>



void partition(const std::string &table_name, const std::vector<std::string> con_columns, const std::vector<std::string> con_columns_nulls,
               const std::vector<std::string> &cat_columns, const std::vector<std::string> cat_columns_nulls, duckdb::Connection &con){

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
            insert_query += "COALESCE (" + col + ", " + std::to_string(avg[null - cat_columns_nulls.begin()]+con_columns_nulls.size()) + "), "+col+" IS NULL, ";
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