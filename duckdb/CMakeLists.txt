cmake_minimum_required(VERSION 3.16)
project(duckdb)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_subdirectory(lib/libduckdb)

#add_library(duckdb_lib SHARED IMPORTED)
#set_property(TARGET duckdb_lib PROPERTY IMPORTED_LOCATION ${lib/libduckdb.dylib})
add_executable(duckdb main.cpp lib/test.cpp triple/Triple_sum.cpp triple/Triple_sum.h triple/Triple_mul.cpp triple/Triple_mul.h triple/Lift.cpp triple/Lift.h triple/From_duckdb.h triple/From_duckdb.cpp triple/Custom_lift.cpp triple/Custom_lift.h triple/Sum_no_lift.cpp triple/Sum_no_lift.h)
target_include_directories(duckdb PRIVATE ${lib/libduckdb/src})

target_compile_features(duckdb PUBLIC cxx_std_23)

#target_include_directories(duckdb PRIVATE lib/)

target_link_libraries(duckdb duckdblib)

#set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0 -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer")
