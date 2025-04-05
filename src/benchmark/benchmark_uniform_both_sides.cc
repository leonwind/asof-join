#include <iostream>
#include <string_view>
#include <vector>
#include <fmt/format.h>
#include "relation.hpp"
#include "benchmark.hpp"
#include <cmath>


void benchmarks::run_uniform_both_sides_benchmark() {
    size_t num_runs = 3;

    const size_t num_stocks = 128;
    const size_t prices_per_stock = 2'000'000;
    const size_t price_sampling_interval = 50;

    const size_t max_timestamp = prices_per_stock * price_sampling_interval;

    const size_t positions_base = 100'000;
    const size_t positions_multiplier = 2;
    const size_t num_position_relations = 11;

    std::cout << "Generate prices with " << num_stocks * prices_per_stock << std::endl;
    Prices prices = generate_uniform_prices(num_stocks * prices_per_stock, max_timestamp, num_stocks);
    const size_t max_num_orders = positions_base * pow(positions_multiplier, num_position_relations);

    {
    OrderBook order_book = generate_uniform_orderbook(max_num_orders, max_timestamp, num_stocks);

    for (size_t i = 0; i <= num_position_relations; ++i) {
        size_t curr_num_positions = positions_base * pow(2, i);
        OrderBook curr_order_book = i < num_position_relations
                ? select_first_n_orders(order_book, curr_num_positions)
                : order_book;

        std::cout << fmt::format("Run[uniform-{}]", curr_num_positions) << std::endl;
        benchmarks::run_benchmark(prices, curr_order_book, num_runs);
    }
    }

    std::vector<double> zipf_skews = {0.5, 1, 1.5, 3};
    for (auto zipf_skew : zipf_skews) {
        OrderBook order_book = generate_zipf_uniform_orderbook(max_num_orders, max_timestamp, num_stocks, zipf_skew);

        for (size_t i = 0; i <= num_position_relations; ++i) {
            size_t curr_num_positions = positions_base * pow(2, i);
            OrderBook curr_order_book = i < num_position_relations
                    ? select_first_n_orders(order_book, curr_num_positions)
                    : order_book;

            std::cout << fmt::format("Run[zipf-{}-{}]", zipf_skew, curr_num_positions) << std::endl;
            benchmarks::run_benchmark(prices, curr_order_book, num_runs);
        }
    }
}
