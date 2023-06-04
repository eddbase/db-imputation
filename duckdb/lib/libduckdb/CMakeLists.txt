cmake_minimum_required(VERSION 3.16)
project(duckdblib)

set(CMAKE_CXX_STANDARD 11)

file(GLOB SOURCES
        src/*.h
        src/*.cpp
        src/*.hpp
        )

add_library(${PROJECT_NAME} SHARED ${SOURCES})
set_target_properties(${PROJECT_NAME} PROPERTIES LINKER_LANGUAGE CXX)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
target_link_libraries(duckdblib PRIVATE Threads::Threads ${CMAKE_DL_LIBS} ${CMAKE_THREAD_LIBS_INIT})

#set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0 -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer")
