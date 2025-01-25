#include "benchmark.hpp"


int main(int argc, const char* argv[]) {
    //if (argc != 3) {
    //    std::cout << "Usage: ./asof_benchmark {path/to/prices.csv} {path/to/positions.csv}"
    //        << std::endl;
    //    return 1;
    //}

    //auto [prices, order_book] = load_data(argv[1], argv[2]);
    //run_benchmark(prices, order_book, /* num_runs */ 3);

    benchmarks::run_diff_zipf_skews_benchmarks();
    //benchmarks::run_small_order_book_contention_benchmark();

    return 0;
}