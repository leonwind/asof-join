#include <iostream>
#include <vector>
#include <fmt/format.h>
#include "relation.hpp"
#include "benchmark.hpp"
#include "zipf_gen.hpp"


void benchmarks::run_increasing_partitions() {
    size_t num_runs = 3;
    size_t num_prices = 50'000'000;
    size_t num_positions = 50'000'000;

    size_t price_sampling_rate = 50;
    size_t max_timestamp = num_prices * price_sampling_rate;

    size_t max_partitions = 1048576; // = 2**20
    for (size_t num_partitions = 1; num_partitions <= max_partitions; num_partitions *= 2) {
        Prices prices = generate_uniform_prices(num_prices, max_timestamp, num_partitions);
        OrderBook order_book = generate_uniform_orderbook(num_positions, max_timestamp, num_partitions);

        std::cout << fmt::format("Num Partitions: {}", num_partitions) << std::endl;
        benchmarks::run_benchmark(prices, order_book, num_runs);
    }
}
