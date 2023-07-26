#include <column_scalability.h>
#include <flight_partition.h>
#include <iostream>
#include <partition.h>
#include <triple/sub.h>
#include <triple/sum/sum.h>
#include <ML/regression.h>
#include <iterator>
void scalability_col_exp(duckdb::Connection &con, const std::vector<std::string> &con_columns, const std::vector<std::string> &cat_columns, const std::vector<std::string> &con_columns_nulls, const std::vector<std::string> &cat_columns_nulls, const std::string &table_name, size_t mice_iters, const std::vector<std::string> &assume_col_con_nulls){
    con.Query("ALTER TABLE "+table_name+" ADD COLUMN n_nulls INTEGER DEFAULT 10;")->Print();
    std::string query = "CREATE TABLE rep AS SELECT ";

    for (auto &col : assume_col_con_nulls)
        query += "CASE WHEN "+col+" IS NULL THEN 1 ELSE 0 END + ";
    query.pop_back();
    query.pop_back();
    con.Query(query+"::INTEGER FROM "+table_name);
    //swap
    //works only with another vector (flat vector)
    con.Query("ALTER TABLE "+table_name+" ALTER COLUMN n_nulls SET DEFAULT 10;")->Print();//not adding b, replace s with rep

    //con.Query("SELECT OP_CARRIER, COUNT(*) FROM join_table WHERE WHEELS_ON_HOUR is not null GROUP BY OP_CARRIER")->Print();

    build_list_of_uniq_categoricals(cat_columns, con, table_name);
    //partition according to n. missing values
    auto begin = std::chrono::high_resolution_clock::now();
    partition_reduce_col_null(table_name, con_columns, con_columns_nulls, cat_columns, cat_columns_nulls, con, assume_col_con_nulls);
    auto end = std::chrono::high_resolution_clock::now();
    std::cout<<"Time partitioning (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

    //run MICE
    //compute main cofactor
    query = "SELECT triple_sum_no_lift(";
    for(auto &col: con_columns){
        query +=col+", ";
    }
    for(auto &col: cat_columns){
        query +=col+", ";
    }
    query.pop_back();
    query.pop_back();
    query += " ) FROM (SELECT * FROM "+table_name+"_complete_0 ";
    for (auto &col: assume_col_con_nulls){
        query += " UNION ALL SELECT * FROM "+table_name+"_complete_"+col;
    }
    query += " UNION ALL SELECT ";
    for(auto &col: con_columns){
        query +=col+", ";
    }
    for(auto &col: cat_columns){
        query +=col+", ";
    }
    query.pop_back();
    query.pop_back();
    query += " FROM "+table_name+"_complete_2)";

    begin = std::chrono::high_resolution_clock::now();
    duckdb::Value full_triple = con.Query(query)->GetValue(0,0);
    full_triple.Print();
    end = std::chrono::high_resolution_clock::now();
    std::cout<<"Time full cofactor (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

    //con.Query("SELECT lift(WHEELS_ON_HOUR) FROM join_table")->Print();

    //start MICE

    for (int mice_iter =0; mice_iter<mice_iters;mice_iter++){
        //continuous cols
        for(auto &col_null : assume_col_con_nulls) {
            std::cout<<"\n\nColumn: "<<col_null<<"\n\n";
            //remove nulls
            std::string delta_query = "SELECT triple_sum_no_lift(";
            for (auto &col: con_columns)
                delta_query += col + ", ";
            for (auto &col: cat_columns)
                delta_query += col + ", ";

            delta_query.pop_back();
            delta_query.pop_back();
            delta_query += " ) FROM (SELECT * FROM " + table_name + "_complete_"+col_null+" UNION ALL (SELECT ";
            for(auto &col: con_columns)
                delta_query +=col+", ";
            for(auto &col: cat_columns)
                delta_query +=col+", ";
            delta_query.pop_back();
            delta_query.pop_back();
            delta_query += " FROM "+table_name+"_complete_2 WHERE "+col_null+"_IS_NULL))";
            //con.Query("SELECT COUNT(*) FROM join_table_complete_WHEELS_ON_HOUR")->Print();
            //con.Query("SELECT COUNT(*) FROM join_table_complete_2 WHERE WHEELS_ON_HOUR_IS_NULL")->Print();

            begin = std::chrono::high_resolution_clock::now();
            duckdb::Value null_triple = con.Query(delta_query)->GetValue(0,0);
            end = std::chrono::high_resolution_clock::now();
            std::cout<<"Time delta cofactor (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

            duckdb::Value train_triple = Triple::subtract_triple(full_triple, null_triple);

            //train
            auto it = std::find(con_columns.begin(), con_columns.end(), col_null);
            size_t label_index = it - con_columns.begin();
            std::cout<<"Label index "<<label_index<<"\n";

            std::vector <double> params = Triple::ridge_linear_regression(train_triple, label_index, 0.001, 0, 1000, true);
            //std::vector <double> params(test.size(), 0);
            //params[0] = 1;

            //predict query
            std::string new_val = "(";
            for(size_t i=0; i< con_columns.size(); i++) {
                if (i==label_index)
                    continue;
                new_val+="("+ std::to_string((float)params[i])+" * "+con_columns[i]+")+";
            }

            new_val.pop_back();

            std::string cat_columns_query;
            query_categorical_num(cat_columns, cat_columns_query,
                                  std::vector<float>(params.begin() + cat_columns.size(), params.end()-1));
            new_val += cat_columns_query + "+random()*"+ std::to_string(params[params.size()-1])+")::FLOAT";

            //update 1 missing value
            std::cout<<"CREATE TABLE rep AS SELECT "+new_val+" AS new_vals FROM "+table_name+"_complete_"+col_null<<"\n";
            begin = std::chrono::high_resolution_clock::now();
            con.Query("CREATE TABLE rep AS SELECT "+new_val+" AS new_vals FROM "+table_name+"_complete_"+col_null);
            con.Query("ALTER TABLE "+table_name+"_complete_"+col_null+" ALTER COLUMN "+col_null+" SET DEFAULT 10;")->Print();//not adding b, replace s with rep
            end = std::chrono::high_resolution_clock::now();
            std::cout<<"Time updating ==1 partition (ms): "<<std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()<<"\n";

            //update 2 missing values
            //new_val = "10";
            //std::cout<<"UPDATE "+table_name+"_complete_2 SET "+col_null+" = "+new_val+" WHERE "+col_null+"_IS_NULL\n";
            //con.Query("UPDATE "+table_name+"_complete_2 SET "+col_null+" = "+new_val+" WHERE "+col_null+"_IS_NULL")->Print();
            std::cout<<"CREATE TABLE rep AS SELECT CASE WHEN "+col_null+"_IS_NULL THEN "+new_val+" ELSE "+col_null+" END AS test FROM "+table_name+"_complete_2\n";
            begin = std::chrono::high_resolution_clock::now();
            con.Query("CREATE TABLE rep AS SELECT CASE WHEN "+col_null+"_IS_NULL THEN "+new_val+" ELSE "+col_null+" END AS test FROM "+table_name+"_complete_2");
            con.Query("ALTER TABLE "+table_name+"_complete_2 ALTER COLUMN "+col_null+" SET DEFAULT 10;")->Print();//not adding b, replace s with rep
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
        std::cout<<"Done..."<<std::endl;
    }
}