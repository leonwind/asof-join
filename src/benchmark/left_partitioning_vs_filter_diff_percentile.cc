#include <vector>
#include "relation.hpp"
#include "benchmark.hpp"

#include "fmt/format.h"


void benchmarks::run_left_partitioning_filtering_diff_percentile() {
    size_t num_runs = 3;

    const size_t num_stocks = 128;
    const size_t prices_per_stock = 2'000'000;
    const size_t price_sampling_interval = 50;

    const size_t max_timestamp = prices_per_stock * price_sampling_interval;

    Prices prices = generate_uniform_prices(num_stocks * prices_per_stock, max_timestamp, num_stocks);

    std::vector<double> percentiles = {
            0, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9,
            0.95, 0.975, 0.99, 0.999, 0.9999
    };

    const size_t num_orders = 12'800'000;

    for (auto percentile : percentiles) {
        auto positions_percentile_min_timestamp = static_cast<size_t>(max_timestamp * percentile);
        OrderBook order_book = generate_uniform_orderbook(
                num_orders,
                positions_percentile_min_timestamp,
                max_timestamp,
                num_stocks);

        std::cout << fmt::format("Percentile: {}", percentile) << std::endl;
        run_benchmark(prices, order_book, num_runs);
    }
}
