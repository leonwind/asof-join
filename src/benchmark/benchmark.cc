#include <iostream>
#include <fmt/format.h>
#include "asof_join.hpp"
#include "relation.hpp"
#include "timer.hpp"
#include "benchmark/benchmark.hpp"


namespace {
uint64_t run_join_return_median_time(ASOFJoin &asof_op, size_t num_runs) {
    std::vector<uint64_t> times(num_runs);
    for (size_t i = 0; i < num_runs; ++i) {
        times[i] = asof_op.join();
        asof_op.result.reset();
    }

    std::sort(times.begin(), times.end());
    return times[num_runs / 2];
}
} // namespace

void benchmarks::run_all() {
    run_diff_zipf_skews_benchmarks();
}

void benchmarks::run_benchmark(Prices &prices, OrderBook &order_book, size_t num_runs) {
    PartitioningLeftASOFJoin left_partitioning(prices, order_book, LESS_EQUAL_THAN, INNER);
    auto left_partitioning_time =
        run_join_return_median_time(left_partitioning, num_runs);
    std::cout << fmt::format("Left partitioning time: {}", left_partitioning_time) << std::endl;

    PartitioningLeftBTreeASOFJoin left_partitioning_btree(prices, order_book, LESS_EQUAL_THAN, INNER);
    auto left_btree_time =
        run_join_return_median_time(left_partitioning_btree, num_runs);
    std::cout << fmt::format("Left BTree time: {}", left_btree_time) << std::endl;

    PartitioningRightASOFJoin right_partitioning(prices, order_book, LESS_EQUAL_THAN, INNER);
    auto right_partitioning_time =
        run_join_return_median_time(right_partitioning, num_runs);
    std::cout << fmt::format("Right partitioning time: {}", right_partitioning_time) << std::endl;

    PartitioningRightBTreeASOFJoin right_partitioning_btree(prices, order_book, LESS_EQUAL_THAN, INNER);
    auto right_btree_time =
        run_join_return_median_time(right_partitioning_btree, num_runs);
    std::cout << fmt::format("Right BTree time: {}", right_btree_time) << std::endl;

    PartitioningBothSortRightASOFJoin both_partitioning_sort_right(
        prices, order_book, LESS_EQUAL_THAN, INNER);
    auto both_partitioning_time =
        run_join_return_median_time(both_partitioning_sort_right, num_runs);
    std::cout << fmt::format("Both partitioning time: {}", both_partitioning_time) << std::endl;

    PartitioningSortedMergeJoin partition_sort(prices, order_book, LESS_EQUAL_THAN, INNER);
    auto partition_sort_time =
            run_join_return_median_time(partition_sort, num_runs);
    std::cout << fmt::format("Sort partitioning time: {}", partition_sort_time) << std::endl;

    uint64_t left_result = left_partitioning.result.value_sum;
    uint64_t right_result = right_partitioning.result.value_sum;
    uint64_t sorted_partitioned_result = partition_sort.result.value_sum;

    if (!(left_result == right_result && right_result == sorted_partitioned_result)) {
        std::cout << fmt::format("RESULTS NOT EQUAL {}, {}, {}", left_result, right_result, sorted_partitioned_result);
    }
}
