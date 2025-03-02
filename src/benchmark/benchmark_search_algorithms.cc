#include <iostream>
#include <vector>
#include <fmt/format.h>
#include <thread>
#include <functional>

#include "tbb/parallel_sort.h"
#include "tbb/parallel_for.h"
#include "tbb/parallel_for_each.h"

#include "searches.hpp"
#include "relation.hpp"
#include "benchmark.hpp"
#include "parallel_multi_map.hpp"


template <typename T>
using fn = std::function<T*(std::vector<T>&, uint64_t)>;


template <typename T>
void benchmark_search_left(Prices& prices, OrderBook& order_book, fn<T> search, std::string_view label,
                           size_t num_runs) {
    std::vector<uint64_t> times(num_runs);

    for (size_t i = 0; i < num_runs; ++i) {
        Timer timer;
        MultiMapTB<T> order_book_lookup(order_book.stock_ids, order_book.timestamps);
        tbb::parallel_for_each(order_book_lookup.begin(), order_book_lookup.end(),
            [&](auto& iter) {
            tbb::parallel_sort(iter.second.begin(), iter.second.end());
        });

        timer.start();
        tbb::parallel_for(tbb::blocked_range<size_t>(0, prices.size),
            [&](tbb::blocked_range<size_t>& range) {
            for (size_t i = range.begin(); i < range.end(); ++i) {
                const auto& stock_id = prices.stock_ids[i];
                if (!order_book_lookup.contains(stock_id)) {
                    continue;
                }

                auto& partition_bin = order_book_lookup[stock_id];
                auto timestamp = prices.timestamps[i];
                auto* match = search(
                    /* data= */ partition_bin,
                    /* target= */ timestamp);

                if (match != nullptr) {
                    uint64_t diff = match->timestamp - timestamp;
                    match->lock_compare_swap_diffs(diff, i);
                }
            }
        });
        times[i] = timer.stop();
    }

    std::sort(times.begin(), times.end());
    std::cout << fmt::format("Left {}: {}[us]", label, times[0]) << std::endl;
}


template <typename T>
void benchmark_search_right(Prices& prices, OrderBook& order_book, fn<T> search, std::string_view label,
                            size_t num_runs) {
    std::vector<uint64_t> times(num_runs);

    size_t res;
    for (size_t i = 0; i < num_runs; ++i) {
        Timer timer;
        MultiMapTB<T> prices_lookup(prices.stock_ids, prices.timestamps);
        tbb::parallel_for_each(prices_lookup.begin(), prices_lookup.end(),
                [&](auto& iter) {
            tbb::parallel_sort(iter.second.begin(), iter.second.end());
        });

        timer.start();
        tbb::parallel_for(tbb::blocked_range<size_t>(0, order_book.size),
                [&](tbb::blocked_range<size_t>& range) {
            size_t local_res = 0;
            for (size_t i = range.begin(); i < range.end(); ++i) {
                auto& stock_id = order_book.stock_ids[i];
                if (!prices_lookup.contains(stock_id)) {
                   continue;
                }

                auto& partition_bin = prices_lookup[stock_id];
                auto timestamp = order_book.timestamps[i];
                auto* match = search(
                    /* data= */ partition_bin,
                    /* target= */ timestamp);

                if (match != nullptr) {
                    ++local_res;
                }
            }
            res += local_res;
        });
        times[i] = timer.stop();
    }

    /// To avoid optimization removing.
    if (res == 0) {
        return;
    }

    std::sort(times.begin(), times.end());
    std::cout << fmt::format("Right {}: {}[us]", label, times[0]) << std::endl;
}


void benchmarks::run_different_search_algorithms() {
    size_t num_runs = 3;

    Prices prices = load_prices("../data/zipf_prices.csv");
    OrderBook order_book = load_order_book("../data/uniform_small/positions_16384.csv");

    std::vector<std::pair<fn<ASOFJoin::LeftEntry>, std::string_view>> left_search_algos = {
         {Search::Binary::greater_equal_than<ASOFJoin::LeftEntry>, "Binary"},
         {Search::Exponential::greater_equal_than<ASOFJoin::LeftEntry>, "Exponential"},
         {Search::Interpolation::greater_equal_than<ASOFJoin::LeftEntry>, "Interpolation"}
    };

    std::vector<std::pair<fn<ASOFJoin::RightEntry>, std::string_view>> right_search_algos = {
         {Search::Binary::less_equal_than<ASOFJoin::RightEntry>, "Binary"},
         {Search::Exponential::less_equal_than<ASOFJoin::RightEntry>, "Exponential"},
         {Search::Interpolation::less_equal_than<ASOFJoin::RightEntry>, "Interpolation"}
    };

    for (auto& left_search_algo : left_search_algos) {
        benchmark_search_left<ASOFJoin::LeftEntry>(
            prices,
            order_book,
            left_search_algo.first,
            left_search_algo.second,
            num_runs);
    }

    for (auto& right_search_algo : right_search_algos) {
        benchmark_search_right<ASOFJoin::RightEntry>(
            prices,
            order_book,
            right_search_algo.first,
            right_search_algo.second,
            num_runs);
    }
}
