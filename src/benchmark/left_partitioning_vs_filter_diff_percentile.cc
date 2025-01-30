#include <iostream>
#include <filesystem>
#include <vector>
#include <fmt/format.h>
#include "relation.hpp"
#include "benchmark.hpp"

namespace fs = std::filesystem;


void benchmarks::run_left_partitioning_filtering_diff_percentile() {
    size_t num_runs = 3;
    std::string small_order_book_data = "../data/uniform_percentile";

    Prices prices = load_prices("../data/prices_128_2000000.csv");

    for (auto& positions_entry : fs::directory_iterator(small_order_book_data)) {
        const auto& positions_path = positions_entry.path().string();
        auto num_positions = util::extract_num_positions(positions_path);
        std::cout << fmt::format("Run [{}]", num_positions) << std::endl;

        OrderBook order_book = load_order_book(positions_path);

        PartitioningLeftASOFJoin normal_left_join(prices, order_book, LESS_EQUAL_THAN, INNER);
        auto normal_left_time = util::run_join_return_best_time(normal_left_join, num_runs);

        PartitioningLeftFilterMinASOFJoin filter_left_join(prices, order_book, LESS_EQUAL_THAN, INNER);
        auto filter_left_time = util::run_join_return_best_time(filter_left_join, num_runs);

        fmt::print("{}: {}\n", normal_left_join.get_strategy_name(), normal_left_time);
        fmt::print("{}: {}\n", filter_left_join.get_strategy_name(), filter_left_time);
    }
}
