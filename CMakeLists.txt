cmake_minimum_required(VERSION 3.15)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project(asof-join)

list(APPEND CMAKE_PREFIX_PATH ${CMAKE_SOURCE_DIR}/.installed/oneTBB)

include(lib/gtest.cmake)

find_package(GTest REQUIRED)
find_package(Threads REQUIRED)
find_package(TBB REQUIRED)

# Set compile flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wno-unused-parameter")
set(CMAKE_CXX_FLAGS_DEBUG "-g -fsanitize=address,leak,undefined -O2")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

include_directories(include)
add_library(asof_join_lib
        src/relation.cc
        src/algorithms/nested_loop_join.cc
        src/algorithms/sorted_merge_join.cc
        src/algorithms/left_partitioning_join.cc
        src/algorithms/right_partitioning_join.cc)

# Tests
add_executable(test_all test/asof_join.cc)
target_link_libraries(test_all asof_join_lib tbb gtest gtest_main)


# Main executable
add_executable(asof_join src/asof_op.cc)
target_link_libraries(asof_join asof_join_lib tbb)
