#include <iostream>
#include <fmt/format.h>
#include "timer.hpp"
#include "log.hpp"
#include "relation.hpp"
#include "asof_join.hpp"


uint64_t run_join_return_median_time(ASOFJoin& asof_op, size_t num_runs) {
    Timer<std::chrono::microseconds> timer;
    timer.start();

    std::vector<uint64_t> times(num_runs);
    for (size_t i = 0; i < num_runs; ++i) {
        timer.lap();
        asof_op.join();
        times[i] = timer.lap();

        asof_op.result.reset();
    }

    std::sort(times.begin(), times.end());
    return times[num_runs / 2];
}


std::pair<Prices, OrderBook> load_data(
        std::string_view prices_path,
        std::string_view positions_path) {
    Timer timer;
    timer.start();
    Prices prices = load_prices(prices_path);
    OrderBook order_book = load_order_book(positions_path);
    auto data_loading_time = timer.lap();

    log(fmt::format("Loaded data in {}", data_loading_time));

    return {std::move(prices), std::move(order_book)};
}


int run_benchmark(std::string_view prices_path, std::string_view positions_path,
        size_t num_runs = 3) {
    auto [prices, order_book] = load_data(prices_path, positions_path);

    PartitioningLeftASOFJoin left_partitioning(prices, order_book, LESS_EQUAL_THAN, INNER);
    auto left_partitioning_time =
        run_join_return_median_time(left_partitioning, num_runs);

    PartitioningRightASOFJoin right_partitioning(prices, order_book, LESS_EQUAL_THAN, INNER);
    auto right_partitioning_time =
        run_join_return_median_time(right_partitioning, num_runs);

    PartitioningSortedMergeJoin partition_sort(prices, order_book, LESS_EQUAL_THAN, INNER);
    auto partition_sort_time =
        run_join_return_median_time(partition_sort, num_runs);

    std::cout << fmt::format("Left partitioning time: {}", left_partitioning_time) << std::endl;
    std::cout << fmt::format("Right partitioning time: {}", right_partitioning_time) << std::endl;
    std::cout << fmt::format("Sort partitioning time: {}", partition_sort_time) << std::endl;

    return 0;
}


int main(int argc, const char* argv[]) {
    if (argc != 3) {
        std::cout << "Usage: ./asof_benchmark {path/to/prices.csv} {path/to/positions.csv}"
            << std::endl;
        return 1;
    }

    return run_benchmark(argv[1], argv[2]);
}