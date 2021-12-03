cmake_minimum_required(VERSION 3.16)
project(factMICE C)

set(CMAKE_C_FLAGS "-O3")
set(CMAKE_C_STANDARD 11)

set(PGSQL_DIRECTORY "/usr/include/postgresql/12/server")
execute_process(
    COMMAND "pg_config" "--includedir-server"
    OUTPUT_VARIABLE 
    PGSQL_DIRECTORY 
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if (APPLE)
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -flat_namespace -undefined suppress")
endif()

add_library(factMICE SHARED library.c library.h ${PGSQL_DIRECTORY}/postgres.h ${PGSQL_DIRECTORY}/fmgr.h)
target_include_directories(factMICE PRIVATE ${PGSQL_DIRECTORY})
