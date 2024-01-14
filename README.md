# PostgreSQL

The PostgreSQL implementation of the ML library as well as the imputation component is available here: 

#### [https://github.com/eddbase/postgres-imputation](https://github.com/eddbase/postgres-imputation)

It also contains examples and build instructions.

-

# DuckDB

The PostgreSQL implementation of the ML library as well as the imputation component is available here:

#### [https://github.com/eddbase/duckdb-imputation](https://github.com/eddbase/duckdb-imputation)

It also contains examples and build instructions.

-
This repository mainly contains scrips to generate the datasets and to run specific experiments, as well as code for imputation with other tools.

## Generate dataset
The folder *make_datasets* contains *\<dataset>\_normalized.py* and *\<dataset>_denormalized.py* to generate the normalized/denormalized dataset used in the experiments. For each column, they contain a parameter **NULL** which should be set as the fraction of missing values to generate.

*dataset\_normalized.py* can be used to generate datasets for experiments 6.1 (without missing values) and 6.3 (with missing values)

*dataset\_denormalized.py* can be used to generate datasets for experiments 6.2 and 6.4. It also includes *SYSTEMDS* and *IMPUTEDB* parameters to generate datasets for SystemDS and ImputeDB instead.

Both scripts expect the dataset in a normalized format (3 tables for Flight, 5 tables for Retailer). Paths and names can be set inside the scripts.

To generate the tables and load the data in Postgres, please use the SQL commands inside *make_datasets/Postgres*.



## Experiments
The folder experiments contain SQL code for the experiments:

### Train

The train subfolder contains the scripts for training over each dataset in PostgreSQL. Requires a normalized dataset without missing values. Methods available in each file are: 

* train_madlib: Uses Madlib to train over the dataset (requires the Madlib library)
* train\_postgres_\<dataset>: trains over the parameters computed in a SQL query. Materialized and non materialized versions of the script are available.
* train\_cofactor_join: ring
* train\_cofactor_factorized: ring + factorized

### Single table

Contains the imputation experiments over a single table. For each dataset, the scripts available are **MICE_partitioned** for the optimized version, **MICE_baseline** for the baseline version. The folder *other systems* contains the Madlib, SystemDS and Mindsdb scripts.

#### SystemDS

To run the imputation with SystemDS, use *dataset\_denormalized.py* to generate the dataset with `SYSTEMDS = true`. It generates two files: a dataset and file containing a sequence of boolean values indicating if a column contains numerical or categorical values.

Edit lines 3 and 4 of `single_table/other systems/systemds.dml` with the number of rows and columns in the dataset (60552738, 34 for flight, 84055817, 44 for retailer). Then run the script with:

`bin/systemds systemds.dml -nvargs X=input_data.csv TYPES=cat_numerical.csv`

#### Madlib
Use *dataset\_denormalized.py* to generate the dataset and import it into PostgreSQL. Then, after installing Madlib run the script inside `single_table/other systems/mice_madlib.sql`.

#### Mindsdb
1. Install Mindsdb and start it using `python -m mindsdb`
2. Make sure the dataset is already loaded inside PostgreSQL
3. Create the output table running the SQL script (`mindsdb_create_<dataset>.sql`)
4. Set PostgreSQL and MindsDB username, password, port and the columns with missing values inside the Python script, then run it

#### ImputeDB
Clone ImputeDB from `https://github.com/mitdbg/imputedb`, then generate the dataset (set `imputedb= True`). Generate the imputedb database with `./imputedb load --db <path database> <path dataset>`. Create a text file with the query (`SELECT * from <table_name>`), then run `./imputedb experiment <path database> <query_path> <output_dir> 1 0 0.9 1`


### Multiple tables
Contains the imputation experiments over a normalized relation. It requires a normalized dataset with missing values inside the fact table. Then, import the tables inside the Postgres and run the SQL code located under *multiple tables/factorized\_\<dataset>*
