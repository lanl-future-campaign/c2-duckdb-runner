cmake_minimum_required(VERSION 3.1)
project(c2-duckdb-runner C CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
            "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
endif ()
set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED)
find_package(DuckDB CONFIG REQUIRED)

add_executable(duckdb-runner main.cc)
target_include_directories(duckdb-runner PUBLIC ${DuckDB_INCLUDE_DIRS})
target_link_libraries(duckdb-runner PUBLIC ${DuckDB_LIBRARIES} Threads::Threads)