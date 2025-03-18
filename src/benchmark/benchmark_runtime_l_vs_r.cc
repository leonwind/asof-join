#include <iostream>
#include <vector>
#include <fmt/format.h>
#include "relation.hpp"
#include "benchmark.hpp"
#include "zipf_gen.hpp"


void benchmarks::run_runtime_l_vs_r() {
    size_t num_runs = 3;
    size_t num_prices = 100'000'000;
    size_t num_positions = 100'000'000;

    //size_t l_r_start = 1'000'000;
    //size_t l_r_increase = 1'000'000;

    size_t price_sampling_rate = 50;

    size_t num_diff_stocks = 128;
    size_t max_timestamp = num_prices * price_sampling_rate;

    Prices prices = generate_uniform_prices(num_prices, max_timestamp, num_diff_stocks);
    OrderBook order_book = generate_uniform_orderbook(num_positions, max_timestamp, num_diff_stocks);

    //for (size_t l = l_r_start; l < num_prices; l += l_r_increase) {
    //    OrderBook order_book_subset = select_first_n_orders(order_book, /* n= */ l);

    //    for (size_t r = l_r_start; r < num_positions; r += l_r_increase) {
    //        Prices prices_subset = select_first_n_prices(prices, /* n= */ r);

    //        std::cout << fmt::format("l={}, r={}", l, r) << std::endl;
    //        benchmarks::run_benchmark(prices_subset, order_book_subset, num_runs);
    //    }
    //}

    for (size_t l = 2; l < num_positions; l *= 2) {
        OrderBook order_book_subset = select_first_n_orders(order_book, /* n= */ l);
        for (size_t r = 2; r < num_prices; r *= 2) {
            Prices prices_subset = select_first_n_prices(prices, /* n= */ r);

            std::cout << fmt::format("l={}, r={}", l, r) << std::endl;
            benchmarks::run_benchmark(prices_subset, order_book_subset, num_runs);
        }
    }
}
