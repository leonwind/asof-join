#include <iostream>
#include <filesystem>
#include <vector>
#include <fmt/format.h>
#include <thread>
#include "tbb/global_control.h"

#include "relation.hpp"
#include "benchmark.hpp"

namespace fs = std::filesystem;


void benchmarks::run_different_num_threads() {
    size_t num_runs = 3;

    Prices prices = load_prices("../data/prices_1_2000000.csv");
    OrderBook order_book = load_order_book("../data/uniform_small/positions_65536.csv");

    const size_t num_cores = std::thread::hardware_concurrency();

    for (size_t threads = 1; threads <= num_cores; ++threads) {
        tbb::global_control control(tbb::global_control::max_allowed_parallelism, threads);

        std::cout << fmt::format("Run num threads: {}", threads) << std::endl;

        PartitioningLeftASOFJoin lock_left_join(prices, order_book, LESS_EQUAL_THAN, INNER);
        auto lock_left_time = util::run_join_return_best_time(lock_left_join, num_runs);

        PartitioningLeftCopyLeftASOFJoin copy_left_join(prices, order_book, LESS_EQUAL_THAN, INNER);
        auto copy_left_time = util::run_join_return_best_time(lock_left_join, num_runs);

        PartitioningLeftFilterMinASOFJoin left_filter_min_join(prices, order_book, LESS_EQUAL_THAN, INNER);
        auto left_filter_time = util::run_join_return_best_time(lock_left_join, num_runs);

        PartitioningRightASOFJoin right_join(prices, order_book, LESS_EQUAL_THAN, INNER);
        auto right_join_time = util::run_join_return_best_time(lock_left_join, num_runs);

        std::cout << fmt::format("{}: {}", lock_left_join.get_strategy_name(), lock_left_time) << std::endl;
        std::cout << fmt::format("{}: {}", copy_left_join.get_strategy_name(), copy_left_time) << std::endl;
        std::cout << fmt::format("{}: {}", left_filter_min_join.get_strategy_name(), left_filter_time) << std::endl;
        std::cout << fmt::format("{}: {}", right_join.get_strategy_name(), right_join_time) << std::endl;
    }
}
