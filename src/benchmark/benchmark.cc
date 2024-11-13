#include <iostream>
#include <fmt/format.h>
#include "asof_join.hpp"
#include "relation.hpp"
#include "timer.hpp"
#include "benchmark/benchmark.hpp"

void benchmarks::run_all() {
    run_diff_zipf_skews_benchmarks();
}

namespace {

uint64_t run_join_return_median_time(ASOFJoin &asof_op, size_t num_runs) {
    Timer<std::chrono::microseconds> timer;
    timer.start();

    std::vector<uint64_t> times(num_runs);
    for (size_t i = 0; i < num_runs; ++i) {
        timer.lap();
        asof_op.join();
        times[i] = timer.lap();

        asof_op.result.reset();
    }

    std::sort(times.begin(), times.end());
    return times[num_runs / 2];
}

} // namespace

void benchmarks::run_benchmark(Prices &prices, OrderBook &order_book, size_t num_runs) {
    PartitioningLeftASOFJoin left_partitioning(prices, order_book, LESS_EQUAL_THAN, INNER);
    auto left_partitioning_time =
            run_join_return_median_time(left_partitioning, num_runs);

    PartitioningRightASOFJoin right_partitioning(prices, order_book, LESS_EQUAL_THAN, INNER);
    auto right_partitioning_time =
            run_join_return_median_time(right_partitioning, num_runs);

    PartitioningSortedMergeJoin partition_sort(prices, order_book, LESS_EQUAL_THAN, INNER);
    auto partition_sort_time =
            run_join_return_median_time(partition_sort, num_runs);

    std::cout << fmt::format("Left partitioning time: {}", left_partitioning_time) << std::endl;
    std::cout << fmt::format("Right partitioning time: {}", right_partitioning_time) << std::endl;
    std::cout << fmt::format("Sort partitioning time: {}", partition_sort_time) << std::endl;
}
