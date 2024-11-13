#ifndef ASOF_JOIN_BENCHMARK_HPP
#define ASOF_JOIN_BENCHMARK_HPP

#include <iostream>
#include <fmt/format.h>
#include "asof_join.hpp"
#include "timer.hpp"


namespace benchmarks {
    void run_diff_zipf_skews_benchmarks() ;

    void run_single_stock_benchmark();

    void run_all();

    void run_benchmark(Prices& prices, OrderBook& order_book, size_t num_runs);
}

#endif // ASOF_JOIN_BENCHMARK_HPP
