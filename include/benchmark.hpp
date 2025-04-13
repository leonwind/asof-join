#ifndef ASOF_JOIN_BENCHMARK_HPP
#define ASOF_JOIN_BENCHMARK_HPP

#include <iostream>
#include <fmt/format.h>
#include "asof_join.hpp"
#include "timer.hpp"


namespace benchmarks {
    void run_diff_zipf_skews_benchmarks();

    void run_uniform_both_sides_benchmark();

    void run_small_order_book_contention_benchmark();

    void run_copying_vs_locking_benchmark();

    void run_left_partitioning_filtering_diff_percentile();

    void run_different_num_threads();

    void run_single_stock_benchmark();

    void run_different_search_algorithms();
    void benchmark_partitioning_search_part();

    void run_runtime_l_vs_r();

    void run_increasing_partitions();

    void run_all();

    void run_benchmark(Prices& prices, OrderBook& order_book, size_t num_runs);
} // namespace benchmarks


namespace benchmarks::util {
    /// Extract the number of positions given its path "positions_X.csv".
    std::string_view extract_num_positions(std::string_view path);

    /// Run join [[num_runs]] times and return the best time.
    uint64_t run_join_return_best_time(ASOFJoin &asof_op, size_t num_runs);

    /// Run join [[num_runs]] times and return the median time.
    uint64_t run_join_return_median_time(ASOFJoin &asof_op, size_t num_runs);
} // namespace benchmarks::util

#endif // ASOF_JOIN_BENCHMARK_HPP
