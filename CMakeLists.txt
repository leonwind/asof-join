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

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wno-unused-parameter")
set(CMAKE_CXX_FLAGS_DEBUG "-g -fsanitize=address,leak,undefined -O2")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")
# Set -DBENCHMARK_MODE to suppress logging while benchmarking.
set(CMAKE_CXX_FLAGS_BENCHMARK "-O3 -DBENCHMARK_MODE")

include_directories(include)

add_library(asof_join_lib
    src/relation.cc
	src/algorithms/nested_loop_join.cc
    src/algorithms/sorted_merge_join.cc
	src/algorithms/partition_sorted_merge_join.cc

    src/algorithms/left_partitioning_join.cc
    src/algorithms/right_partitioning_join.cc
	src/algorithms/partitioning_both_sort_left.cc
	src/algorithms/left_partitioning_filter_min.cc
	src/algorithms/right_partitioning_filter_min.cc

    src/algorithms/left_partitioning_btree.cc
    src/algorithms/right_partitioning_btree.cc

	src/algorithms/copy_left_partitioning_join.cc)

target_link_libraries(asof_join_lib PUBLIC TBB::tbb fmt atomic)

add_library(asof_benchmark_lib
    src/benchmark/benchmark.cc
    src/benchmark/diff_zipf_skews.cc
	src/benchmark/small_order_book_contention.cc
	src/benchmark/copying_vs_locking.cc
	src/benchmark/left_partitioning_vs_filter_diff_percentile.cc
	src/benchmark/benchmark_threads_scalability.cc
	src/benchmark/benchmark_search_algorithms.cc)
# Link main lib sources into benchmark lib
target_link_libraries(asof_benchmark_lib asof_join_lib)

# Executable
add_executable(asof_join src/asof_op.cc)
target_link_libraries(asof_join
    asof_join_lib)

# Benchmarks
add_executable(asof_benchmark src/asof_benchmark.cc)
target_link_libraries(asof_benchmark
    asof_benchmark_lib)

# Tests
add_executable(test_all
    test/asof_join_test.cc
    test/btree_test.cc
    test/parallel_multi_map_test.cc
    test/tuple_buffer_test.cc
	test/searches_test.cc)
target_link_libraries(test_all
    asof_join_lib
    gtest
    gtest_main)
