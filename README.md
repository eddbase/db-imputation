# Instruction

## Generate dataset
The folder *make_datasets* contains *\<dataset>\_normalized.py* and *\<dataset>_denormalized.py* to generate the normalized/denormalized dataset used in the experiments. For each column, they contain a parameter **NULL** which should be set as the fraction of missing values to generate.

*dataset\_normalized.py* can be used to generate datasets for experiments 6.1 (without missing values) and 6.3 (with missing values)

*dataset\_denormalized.py* can be used to generate datasets for experiments 6.2 and 6.4. It also includes *SYSTEMDS* and *IMPUTEDB* parameters to generate datasets for SystemDS and ImputeDB instead.

Both scripts expect the dataset in a normalized format(3 tables for Flight, 5 tables for Retailer). Paths and names can be set inside the scripts.

To generate the tables and load the data in Postgres, please use the SQL commands inside *make_datasets/Postgres*.

## Postgres library

The Postgres library requires CMake, BLAS and LAPACK packages, already installed on most systems.

1. Make sure `pg_config --includedir-server` and `pg_config --pkglibdir` work properly, otherwise set the variables *PGSQL\_INCLUDE\_DIRECTORY* and *PGSQL\_LIB\_DIRECTORY* inside CMakeLists.txt according to the installation path of PostgreSQL in your system.
2. Run `cmake .`
3. Run `make`
4. Run `sql/create_UDFs.sh` to export the functions inside Postgres

## DuckDB
**Imputation experiments on DuckDB require the library available** [here](https://anonymous.4open.science/r/duckdb_swap-30F7/) , placed under lib/duckdb.
Other requirements are BLAS, LAPACK and Boost. The folder DuckDB contains the library and the code for every experiment.

1. Clone [https://anonymous.4open.science/r/duckdb_swap-30F7/](https://anonymous.4open.science/r/duckdb_swap-30F7/) and place it in `lib/duckdb`
2. Set inside `main.cpp` the experiments you want to run (boolean variables at the beginning)
2. Run `cmake .`
3. Run `make`
4. Run `./DuckDB_imputation`


## Experiments
The folder experiments contain the PostgreSQL SQL code for the experiments:

### Train

The train subfolder contains the scripts for training over each dataset. Requires a normalized dataset without missing values. Methods available in each file are: 

* train_madlib: Uses Madlib to train over the dataset (requires the Madlib library)
* train\_postgres_\<dataset>: trains over the parameters computed in a SQL query. Materialized and non materialized versions of the script are available.
* train\_cofactor_join: ring
* train\_cofactor_factorized: ring + factorized

### Single table

Contains the imputation experiments over a single table. For each dataset, the scripts available are **MICE_partitioned** for the optimized version, **MICE_baseline** for the baseline version. The folder *other systems* contains the Madlib and SystemDS versions.

#### SystemDS

Edit lines 3 and 4 of `single_table/other systems/systemds.dml` with the number of rows and columns in the dataset (60552738, 34 for flight, 84055817, 44 for retailer, ...). Then run it with:

`bin/systemds systemds.dml -nvargs X=input_data.csv TYPES=cat_numerical.csv`

### Multiple tables
Contains the imputation experiments over a normalized relation. Generate a normalized dataset setting the null quantity for each column, import the tables inside the Postgres and run the SQL code located under *multiple tables/factorized\_\<dataset>*
