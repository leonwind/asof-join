#include <iostream>
#include <string_view>
#include <filesystem>
#include <vector>
#include <fmt/format.h>
#include "relation.hpp"
#include "benchmark.hpp"

namespace fs = std::filesystem;


void benchmarks::run_diff_zipf_skews_benchmarks() {
    size_t num_runs = 3;
    std::vector<std::string_view> positions_dirs = {
        "../data/uniform",
        "../data/zipf_1_1",
        "../data/zipf_1_3",
        "../data/zipf_1_5",
        "../data/zipf_2"
    };

    Prices prices = load_prices("../data/prices_small.csv");
    for (auto positions_dir : positions_dirs) {
        auto distribution_name =
                positions_dir.substr(8, positions_dir.size());

        for (auto& positions_entry : fs::directory_iterator(positions_dir)) {
            const auto& positions_path = positions_entry.path().string();
            auto num_positions = util::extract_num_positions(positions_path);
            std::cout << fmt::format("Run [{}-{}]", distribution_name, num_positions) << std::endl;

            OrderBook order_book = load_order_book(positions_path);

            run_benchmark(prices, order_book, num_runs);
        }
    }
}
