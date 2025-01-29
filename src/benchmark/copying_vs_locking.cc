#include <iostream>
#include <filesystem>
#include <vector>
#include <fmt/format.h>
#include "relation.hpp"
#include "benchmark.hpp"

namespace fs = std::filesystem;
//using namespace benchmarks::util;


void benchmarks::run_copying_vs_locking_benchmark() {
    size_t num_runs = 3;
    std::string small_order_book_data = "../data/uniform_small";

    Prices prices = load_prices("../data/zipf_prices.csv");

    for (auto& positions_entry : fs::directory_iterator(small_order_book_data)) {
        const auto& positions_path = positions_entry.path().string();
        auto num_positions = util::extract_num_positions(positions_path);
        std::cout << fmt::format("Run [{}]", num_positions) << std::endl;

        OrderBook order_book = load_order_book(positions_path);

        PartitioningLeftASOFJoin lock_left_join(prices, order_book, LESS_EQUAL_THAN, INNER);
        auto lock_left_time = util::run_join_return_best_time(lock_left_join, num_runs);

        PartitioningLeftCopyLeftASOFJoin copy_left_join(prices, order_book, LESS_EQUAL_THAN, INNER);
        auto copy_left_time = util::run_join_return_best_time(copy_left_join, num_runs);

        std::cout << fmt::format("{}: {}", lock_left_join.get_strategy_name(), lock_left_time) << std::endl;
        std::cout << fmt::format("{}: {}", copy_left_join.get_strategy_name(), copy_left_time) << std::endl;
    }
}
