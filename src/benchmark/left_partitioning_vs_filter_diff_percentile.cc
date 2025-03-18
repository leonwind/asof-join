#include <vector>
#include "relation.hpp"
#include "benchmark.hpp"


void benchmarks::run_left_partitioning_filtering_diff_percentile() {
    size_t num_runs = 3;

    const size_t num_stocks = 128;
    const size_t prices_per_stock = 2'000'000;
    const size_t price_sampling_interval = 50;

    const size_t max_timestamp = prices_per_stock * price_sampling_interval;

    Prices prices = generate_uniform_prices(num_stocks * prices_per_stock, max_timestamp, num_stocks);

    std::vector<double> percentiles = {
            1, 0.95, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1,
            0.05, 0.025, 0.0125, 0.0001, 0.00001
    };

    const size_t num_orders = 6'400'000;

    for (auto percentile : percentiles) {
        auto positions_percentile_max_timestamp = static_cast<size_t>(max_timestamp * percentile);
        OrderBook order_book = generate_uniform_orderbook(
                num_orders,
                positions_percentile_max_timestamp,
                num_stocks);

        run_benchmark(prices, order_book, num_runs);
    }
}
