# Instruction

## Generate dataset
The folder *make_datasets* contains *\<dataset>\_normalized.py* and *\<dataset>_denormalized.py* to generate the normalized/denormalized dataset used in the experiments. Both contain a parameter **NULL** which should be set as the fraction of missing values to generate.

*dataset\_normalized.py* can be used to generate datasets for experiments 6.1 (without missing values) and 6.3 (with missing values)

*dataset\_denormalized.py* can be used to generate datasets for experiment 6.2. It also includes *SYSTEMDS* and *IMPUTEDB* parameter to generate datasets for SystemDS and ImputeDB instead.

Both script expects the dataset in a normalized format(3 tables for Flight, 5 tables for Retailer). Path and names can be set inside the scripts.

To generate the tables and load the data in Postgres, please use the SQL commands inside the *Postgres* subfolder. It includes also commands for loading the air quality dataset. Execute the \copy commands one at a time.

## Postgres library

The Postgres library requires CMake, BLAS and LAPACK packages, already installed on most systems.

1. Make sure `pg_config --includedir-server` and `pg_config --pkglibdir` work properly, otehrwise set the variables *PGSQL\_INCLUDE\_DIRECTORY* and *PGSQL\_LIB\_DIRECTORY* inside CMakeLists.txt according to the installation path of PostgreSQL in your system.
2. Run `cmake .`
3. Run `make`
4. Run `sql/create_UDFs.sh` to export the functions inside Postgres
5. Install tablefunc: inside a Postgres shell run `CREATE EXTENSION tablefunc;`

## DuckDB library
TODO

## Postgres experiments
The folder experiments_postgres contains the SQL code for the experiments:

### Train

The train subfolder contains the scripts for training over each dataset. Requires a normalized dataset without missing values. Methods available are in each file are: 

* train_madlib: Uses Madlib to train over the dataset (requires the Madlib library)
* train\_postgres_\<dataset>: trains over the parameters computed in a SQL query. Materialized and non materialized versions of the script are available.
* train\_cofactor_join: ring
* train\_cofactor_factorized: ring + factorized

### Single table

Contains the imputation experiments over a single table. For each datasets the scripts available are **MICE_partitioned** for the optimized version, **MICE_baseline** for the baseline version. The folder *other systems* contains 