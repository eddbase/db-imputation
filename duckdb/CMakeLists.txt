cmake_minimum_required(VERSION 3.1)
project(Test)
set(CMAKE_BUILD_PARALLEL_LEVEL 8)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)


find_package(BLAS)
find_package(LAPACK)
if(LAPACK_FOUND AND BLAS_FOUND)
    set(lapackblas_libraries ${BLAS_LIBRARIES} ${LAPACK_LIBRARIES})
else()
    panic()
endif()

include(ExternalProject)

set(ENABLE_DOCTESTS ON)

if(ENABLE_DOCTESTS)
    add_definitions(-DENABLE_DOCTEST_IN_LIBRARY)
    include(FetchContent)
    FetchContent_Declare(
            DocTest
            GIT_REPOSITORY "https://github.com/onqtam/doctest"
    )

    FetchContent_MakeAvailable(DocTest)
    include_directories(${DOCTEST_INCLUDE_DIR})
endif()

ExternalProject_Add(
        duckdb_ep
        PREFIX "${CMAKE_BINARY_DIR}/lib/duckdb"
        SOURCE_DIR "${CMAKE_SOURCE_DIR}/lib/duckdb"
        INSTALL_DIR "${CMAKE_BINARY_DIR}/lib/duckdb/install"
        CMAKE_ARGS
        -G${CMAKE_GENERATOR}
        -DCMAKE_BUILD_PARALLEL_LEVEL=${CMAKE_BUILD_PARALLEL_LEVEL}
        -DCMAKE_BUILD_TYPE=Release
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
        BUILD_COMMAND ${CMAKE_MAKE_PROGRAM} -j${CMAKE_BUILD_PARALLEL_LEVEL} duckdb_static duckdb
        BUILD_BYPRODUCTS
        <INSTALL_DIR>/lib/libduckdb_static.a
)

ExternalProject_Get_Property(duckdb_ep INSTALL_DIR)
set(DUCKDB_LIBRARY_PATH "${INSTALL_DIR}/lib/libduckdb_static.a")
set(DUCKDB_INCLUDE_DIR "${INSTALL_DIR}/include")
file(MAKE_DIRECTORY ${DUCKDB_INCLUDE_DIR})

#add_library(duckdb STATIC IMPORTED)
add_library(duckdb SHARED IMPORTED)
set_property(TARGET duckdb PROPERTY IMPORTED_LOCATION ${DUCKDB_LIBRARY_PATH})
set_property(TARGET duckdb APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${DUCKDB_INCLUDE_DIR})
#target_link_libraries(duckdb
#        INTERFACE "${INSTALL_DIR}/lib/libduckdb_static.a"
#        )
#set_target_properties(duckdb PROPERTIES IMPORTED_LOCATION ${DUCKDB_LIB_PATH})
set_target_properties(duckdb PROPERTIES IMPORTED_LOCATION ${INSTALL_DIR}/lib/libduckdb.dylib)

add_dependencies(duckdb duckdb_ep)

add_executable(Test main.cpp triple/Triple_sum.cpp triple/Triple_sum.h triple/Triple_mul.cpp triple/Triple_mul.h triple/Lift.cpp triple/Lift.h triple/From_duckdb.h triple/From_duckdb.cpp triple/Custom_lift.cpp triple/Custom_lift.h triple/Sum_no_lift.cpp triple/Sum_no_lift.h triple/Triple_sub.cpp triple/Triple_sub.h ML/Regression.cpp ML/Regression.h ML/Regression_predict.cpp ML/Regression_predict.h triple/helper.cpp triple/helper.h partition.cpp partition.h tests/test_lift.cpp tests/test_sum.cpp tests/test_mul.cpp ML/lda.cpp ML/lda.h ML/utils.cpp ML/utils.h ML/lda_impute.cpp ML/lda_impute.h tests/test_lda.cpp experiments/flight_partition.cpp experiments/flight_partition.h experiments/flight_baseline.cpp experiments/flight_baseline.h experiments/train_flight.cpp experiments/train_flight.h experiments/train_retailer.cpp experiments/train_retailer.h)
target_link_libraries(Test duckdb)
target_link_libraries(Test doctest)
target_link_libraries(Test ${lapackblas_libraries})
target_compile_options(Test PRIVATE ${LAPACK_LINKER_FLAGS})

set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME OFF)
find_package(Boost REQUIRED)

if(Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
#    target_link_libraries(Test ${Boost_LIBRARIES})
endif()