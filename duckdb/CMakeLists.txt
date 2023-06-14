cmake_minimum_required(VERSION 3.1)
project(Test)
set(CMAKE_BUILD_PARALLEL_LEVEL 8)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(ExternalProject)

ExternalProject_Add(
        duckdb_ep
        PREFIX "${CMAKE_BINARY_DIR}/lib/duckdb"
        SOURCE_DIR "${CMAKE_SOURCE_DIR}/lib/duckdb"
        INSTALL_DIR "${CMAKE_BINARY_DIR}/lib/duckdb/install"
        CMAKE_ARGS
        -G${CMAKE_GENERATOR}
        -DCMAKE_BUILD_PARALLEL_LEVEL=${CMAKE_BUILD_PARALLEL_LEVEL}
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_CXX_FLAGS=-std=c++17
        -DCMAKE_CXX_STANDARD=17
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/lib/duckdb/install
        -DCMAKE_MODULE_PATH=${CMAKE_MODULE_PATH}
        -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}
        -DLEAN=ON
        DOWNLOAD_COMMAND ""
        UPDATE_COMMAND ""
        BUILD_COMMAND ${CMAKE_MAKE_PROGRAM} -j${CMAKE_BUILD_PARALLEL_LEVEL} duckdb_static
        BUILD_BYPRODUCTS
        <INSTALL_DIR>/lib/libduckdb_static.a
)

ExternalProject_Get_Property(duckdb_ep INSTALL_DIR)
set(DUCKDB_LIBRARY_PATH "${INSTALL_DIR}/lib/libduckdb_static.a")
set(DUCKDB_INCLUDE_DIR "${INSTALL_DIR}/include")
file(MAKE_DIRECTORY ${DUCKDB_INCLUDE_DIR})

#add_library(duckdb STATIC IMPORTED)
add_library(duckdb SHARED IMPORTED )
set_property(TARGET duckdb PROPERTY IMPORTED_LOCATION ${DUCKDB_LIBRARY_PATH})
set_property(TARGET duckdb APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${DUCKDB_INCLUDE_DIR})
#target_link_libraries(duckdb
#        INTERFACE "${INSTALL_DIR}/lib/libduckdb_static.a"
#        )
#set_target_properties(duckdb PROPERTIES IMPORTED_LOCATION ${DUCKDB_LIB_PATH})
set_target_properties(duckdb PROPERTIES IMPORTED_LOCATION /Users/massimo/Downloads/new_proj/cmake-build-debug/lib/duckdb/install/lib/libduckdb.dylib)

add_dependencies(duckdb duckdb_ep)

add_executable(Test main.cpp triple/Triple_sum.cpp triple/Triple_sum.h triple/Triple_mul.cpp triple/Triple_mul.h triple/Lift.cpp triple/Lift.h triple/From_duckdb.h triple/From_duckdb.cpp triple/Custom_lift.cpp triple/Custom_lift.h triple/Sum_no_lift.cpp triple/Sum_no_lift.h triple/Triple_sub.cpp triple/Triple_sub.h ML/Regression.cpp ML/Regression.h ML/Regression_predict.cpp ML/Regression_predict.h)
target_link_libraries(Test duckdb)