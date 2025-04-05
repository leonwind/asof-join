#include "benchmark.hpp"


int main(int argc, const char* argv[]) {
    //if (argc != 3) {
    //    std::cout << "Usage: ./asof_benchmark {path/to/prices.csv} {path/to/positions.csv}"
    //        << std::endl;
    //    return 1;
    //}

    //auto [prices, order_book] = load_data(argv[1], argv[2]);
    //run_benchmark(prices, order_book, /* num_runs */ 3);

    //benchmarks::run_diff_zipf_skews_benchmarks();
    //benchmarks::run_small_order_book_contention_benchmark();
    //benchmarks::run_copying_vs_locking_benchmark();
    //benchmarks::run_left_partitioning_filtering_diff_percentile();
    //benchmarks::run_different_num_threads();

    benchmarks::run_different_search_algorithms();
    //benchmarks::benchmark_partitioning_search_part();

    //benchmarks::run_runtime_l_vs_r();

    //benchmarks::run_uniform_both_sides_benchmark();

    return 0;
}