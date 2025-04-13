#include <iostream>
#include <vector>
#include <fmt/format.h>
#include "relation.hpp"
#include "benchmark.hpp"
#include "zipf_gen.hpp"


void benchmarks::run_runtime_l_vs_r() {
    size_t num_runs = 3;
    size_t num_prices = 268435456;
    size_t num_positions = 268435456;

    size_t price_sampling_rate = 50;

    size_t num_diff_stocks = 128;
    size_t max_timestamp = num_prices * price_sampling_rate;

    Prices prices = generate_uniform_prices(num_prices, max_timestamp, num_diff_stocks);
    OrderBook order_book = generate_uniform_orderbook(num_positions, max_timestamp, num_diff_stocks);

    for (size_t l = 1; l <= num_positions; l *= 2) {
        OrderBook order_book_subset = l < num_positions
                ? select_first_n_orders(order_book, /* n= */ l)
                : order_book;
        for (size_t r = 1; r <= num_prices; r *= 2) {
            Prices prices_subset = r < num_prices
                    ? select_first_n_prices(prices, /* n= */ r)
                    : prices;

            std::cout << fmt::format("l={}, r={}", l, r) << std::endl;
            benchmarks::run_benchmark(prices_subset, order_book_subset, num_runs);
        }
    }
}
