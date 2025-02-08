#include <iostream>
#include <fmt/format.h>
#include "asof_join.hpp"
#include "relation.hpp"
#include "timer.hpp"
#include "benchmark.hpp"

void benchmarks::run_all() {
    run_diff_zipf_skews_benchmarks();
}

std::string_view benchmarks::util::extract_num_positions(std::string_view path) {
    std::string_view prefix = "positions_";
    std::string_view suffix = ".csv";

    size_t begin = path.find(prefix) + prefix.size();
    size_t end = path.find(suffix);

    return path.substr(begin, end);
}

uint64_t benchmarks::util::run_join_return_best_time(ASOFJoin &asof_op, size_t num_runs) {
    std::vector<uint64_t> times(num_runs);
    for (size_t i = 0; i < num_runs; ++i) {
        Timer timer;
        timer.start();
        asof_op.join();
        times[i] = timer.stop();
        asof_op.result.reset();
    }

    std::sort(times.begin(), times.end());
    return times[0];
}

uint64_t benchmarks::util::run_join_return_median_time(ASOFJoin &asof_op, size_t num_runs) {
    std::vector<uint64_t> times(num_runs);
    for (size_t i = 0; i < num_runs; ++i) {
        Timer timer;
        timer.start();
        asof_op.join();
        times[i] = timer.stop();
        asof_op.result.reset();
    }

    std::sort(times.begin(), times.end());
    return times[num_runs / 2];
}

void benchmarks::run_benchmark(Prices &prices, OrderBook &order_book, size_t num_runs) {
    PartitioningRightASOFJoin right_partitioning(prices, order_book, LESS_EQUAL_THAN, INNER);
    auto right_partitioning_time = util::run_join_return_best_time(right_partitioning, num_runs);
    std::cout << fmt::format("{}: {}", right_partitioning.get_strategy_name(), right_partitioning_time) << std::endl;

    PartitioningLeftASOFJoin left_partitioning(prices, order_book, LESS_EQUAL_THAN, INNER);
    auto left_partitioning_time =
        util::run_join_return_best_time(left_partitioning, num_runs);
    std::cout << fmt::format("{}: {}", left_partitioning.get_strategy_name(), left_partitioning_time) << std::endl;

    //PartitioningBothSortLeftASOFJoin both_partitioning_sort_left(
    //    prices, order_book, LESS_EQUAL_THAN, INNER);
    //auto both_partitioning_time= util::run_join_return_best_time(both_partitioning_sort_left, num_runs);
    //std::cout << fmt::format("{}: {}",both_partitioning_sort_left.get_strategy_name(), both_partitioning_time) << std::endl;

    PartitioningSortedMergeJoin partition_sort(prices, order_book, LESS_EQUAL_THAN, INNER);
    auto partition_sort_time=
        util::run_join_return_best_time(partition_sort, num_runs);
    std::cout << fmt::format("{}: {}", partition_sort.get_strategy_name(), partition_sort_time) << std::endl;

    PartitioningRightFilterMinASOFJoin filter_min_right_partition(prices, order_book, LESS_EQUAL_THAN, INNER);
    auto right_partitioning_filter_min_time = util::run_join_return_best_time(filter_min_right_partition, num_runs);
    std::cout << fmt::format("{}: {}", filter_min_right_partition.get_strategy_name(), right_partitioning_filter_min_time)
              << std::endl;

    PartitioningLeftFilterMinASOFJoin filter_min_left_partition(prices, order_book, LESS_EQUAL_THAN, INNER);
    auto left_partitioning_filter_min_time = util::run_join_return_best_time(filter_min_left_partition, num_runs);
    std::cout << fmt::format("{}: {}", filter_min_left_partition.get_strategy_name(), left_partitioning_filter_min_time)
              << std::endl;

    //uint64_t right_result = right_partitioning.result.value_sum;
    //uint64_t left_result = left_partitioning.result.value_sum;
    //uint64_t sorted_partitioned_result = partition_sort.result.value_sum;

    //if (!(right_result == left_result && left_result == sorted_partitioned_result)) {
    //    std::cout << fmt::format("RESULTS NOT EQUAL {}, {}, {}", right_result, left_result, sorted_partitioned_result)
    //              << std::endl;
    //}
}
