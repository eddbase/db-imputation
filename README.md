# Instruction

## Generate dataset
The folder *make_datasets* contains *\<dataset>\_normalized.py* and *\<dataset>_denormalized.py* to generate the normalized/denormalized dataset used in the experiments. Both contain a parameter **NULL** which should be set as the fraction of missing values to generate.

*dataset\_normalized.py* can be used to generate datasets for experiments 6.1 (without missing values) and 6.3 (with missing values)

*dataset\_denormalized.py* can be used to generate datasets for experiment 6.2. It also includes *SYSTEMDS* and *IMPUTEDB* parameter to generate datasets for SystemDS and ImputeDB instead.

Both script expects the dataset in a normalized format(3 tables for Flight, 5 tables for Retailer). Path and names can be set inside the scripts.

To generate the tables and load the data in Postgres, please use the SQL commands inside the *Postgres* subfolder.

## Postgres library

The Postgres library requires CMake, BLAS and LAPACK packages, already installed on most systems.

1. Make sure `pg_config --includedir-server` and `pg_config --pkglibdir` work properly, otehrwise set the variables *PGSQL\_INCLUDE\_DIRECTORY* and *PGSQL\_LIB\_DIRECTORY* inside CMakeLists.txt according to the installation path of PostgreSQL in your system.
2. Run `cmake .`
3. Run `make`
4. Run `create_UDFs.sh` inside the sql folder to export the functions inside Postgres