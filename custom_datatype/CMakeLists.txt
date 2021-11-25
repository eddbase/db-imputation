cmake_minimum_required(VERSION 3.16)
project(factMICE C)

set(CMAKE_C_FLAGS "-O3")
set(CMAKE_C_STANDARD 11)

set(PGSQL_DIRECTORY "/usr/include/postgresql/12/server")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_library(factMICE SHARED library.c library.h ${PGSQL_DIRECTORY}/postgres.h ${PGSQL_DIRECTORY}/fmgr.h)
target_include_directories(factMICE PRIVATE ${PGSQL_DIRECTORY})
