cmake_minimum_required(VERSION 3.16)
project(factML C)

set(HASHTAB_FLAGS "-DHAVE_STDLIB_H -DHAVE_STRING_H -DHAVE_LIMITS_H -D HAVE_INTTYPES_H -DHAVE_STDINT_H -DHAVE_MALLOC_H")

if (APPLE)
    set(CMAKE_OSX_DEPLOYMENT_TARGET "13" CACHE STRING "Minimum OS X deployment version")
    set(HASHTAB_FLAGS "-DHAVE_STDLIB_H -DHAVE_STRING_H -DHAVE_LIMITS_H -D HAVE_INTTYPES_H -DHAVE_STDINT_H")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -flat_namespace -undefined suppress")
endif()

find_package(BLAS)
find_package(LAPACK)
if(LAPACK_FOUND AND BLAS_FOUND)
    set(lapackblas_libraries ${BLAS_LIBRARIES} ${LAPACK_LIBRARIES})
else()
    panic()
endif()

#set(CMAKE_C_FLAGS "-O3 ${HASHTAB_FLAGS} -pedantic")
set(CMAKE_C_FLAGS "-O3 ${HASHTAB_FLAGS}")
set(CMAKE_C_STANDARD 11)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY test/bin)

set(PGSQL_INCLUDE_DIRECTORY "/usr/include/postgresql/12/server")
execute_process(
    COMMAND "pg_config" "--includedir-server"
    OUTPUT_VARIABLE
    PGSQL_INCLUDE_DIRECTORY
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
set(PGSQL_LIB_DIRECTORY "/usr/lib/postgresql/12/server")
execute_process(
    COMMAND "pg_config" "--pkglibdir"
    OUTPUT_VARIABLE
    PGSQL_LIB_DIRECTORY
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_library(factML SHARED
    src/relation.c
        src/cofactor.c
    src/serializer.c
        src/lda.c
    src/gradient_descent.c
        src/nb_aggregates.c
        src/naive_bayes.c src/qda.c include/datatype.h src/datatype.c)
target_include_directories(factML PRIVATE ${PGSQL_INCLUDE_DIRECTORY} PRIVATE include)
target_include_directories(factML PRIVATE ${PGSQL_INCLUDE_DIRECTORY}/catalog PRIVATE include)

target_link_libraries(factML ${lapackblas_libraries})
target_compile_options(factML PRIVATE ${LAPACK_LINKER_FLAGS})


add_executable(test_serializer
    src/serializer.c
    test/src/test_serializer.c
)
target_include_directories(test_serializer PRIVATE ${PGSQL_INCLUDE_DIRECTORY} PRIVATE include)
target_link_libraries(test_serializer ${PGSQL_LIB_DIRECTORY}/libpgport.a ${lapackblas_libraries})
target_compile_options(test_serializer PRIVATE ${LAPACK_LINKER_FLAGS})
#set(CMAKE_VERBOSE_MAKEFILE ON)
