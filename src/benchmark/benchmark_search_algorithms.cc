#include <iostream>
#include <vector>
#include <fmt/format.h>
#include <thread>
#include <functional>

#include "tbb/parallel_sort.h"
#include "tbb/parallel_for.h"
#include "tbb/parallel_for_each.h"

#include "asof_join.hpp"
#include "uniform_gen.hpp"
#include "searches.hpp"
#include "benchmark.hpp"
#include "parallel_multi_map.hpp"
#include "zipf_gen.hpp"


#define PRICE_SAMPLING_RATE 30


namespace {
    struct TestEntry;
    using fn = std::function<TestEntry*(std::vector<TestEntry>&, uint64_t)>;

    struct TestEntry: ASOFJoin::JoinEntry {
        uint64_t key;
        uint64_t value;

        explicit TestEntry(uint64_t key_val): key(key_val), value(key_val) {}
        TestEntry(uint64_t key, uint64_t value): key(key), value(value) {}

        [[nodiscard]] inline uint64_t get_key() const override {
            return key;
        }

        std::string str() {
            return fmt::format("[{}, {}]", key, value);
        }
    };

    std::vector<TestEntry> generate_random_price_list(size_t num_prices) {
        std::vector<TestEntry> data;

        for (size_t i = 0; i < num_prices; ++i) {
            data.emplace_back(uniform::gen_int(num_prices * PRICE_SAMPLING_RATE));
        }

        tbb::parallel_sort(data.begin(), data.end());
        return data;
    }

    std::vector<TestEntry> generate_zipfian_price_list(size_t num_prices, double zipf_skew) {
        std::mt19937 rng(std::random_device{}());
        Zipf zipf_gen(num_prices, zipf_skew);
        std::vector<TestEntry> data;

        for (size_t i = 0; i < num_prices; ++i) {
            data.emplace_back(zipf_gen(rng));
        }

        tbb::parallel_sort(data.begin(), data.end());
        return data;
    }

    std::vector<TestEntry> generate_random_orderbook_list(size_t num_orders, size_t max_timestamp) {
        std::vector<TestEntry> data;

        for (size_t i = 0; i < num_orders; ++i) {
            data.emplace_back(uniform::gen_int(max_timestamp));
        }

        tbb::parallel_sort(data.begin(), data.end());
        return data;
    }

    std::vector<TestEntry> generate_zipfian_orderbook_list(size_t num_orders, double zipf_skew) {
        std::mt19937 rng(std::random_device{}());
        Zipf<uint64_t, double> zipf_gen(num_orders, zipf_skew);
        std::vector<TestEntry> data;

        for (size_t i = 0; i < num_orders; ++i) {
            data.emplace_back(zipf_gen(rng));

            if (i < 20) {
                std::cout << data[i].get_key() << ",";
            }
            std::cout << std::endl;
        }

        tbb::parallel_sort(data.begin(), data.end());
        return data;
    }

} // namespace

void benchmark_search_left(std::vector<TestEntry>& prices, std::vector<TestEntry>& order_book,
                           const fn& search, std::string_view label, size_t num_runs) {
    std::vector<uint64_t> times(num_runs);

    //PerfEventBlock e(prices.size() * num_runs);
    size_t matches = 0;
    for (size_t i = 0; i < num_runs; ++i) {
        Timer timer;

        timer.start();
        for (auto& timestamp_target : prices) {
            auto* match = search(order_book, timestamp_target.key);
            matches += match != nullptr;
        }
        times[i] = timer.stop();
    }

    /// To avoid optimization removing.
    if (matches == 0) {
        return;
    }

    std::sort(times.begin(), times.end());
    std::cout << fmt::format("Left {}: {}[us]", label, times[0]) << std::endl;
}


void benchmark_search_right(std::vector<TestEntry>& prices, std::vector<TestEntry>& order_book,
                            const fn& search, std::string_view label, size_t num_runs) {
    std::vector<uint64_t> times(num_runs);

    //PerfEventBlock e(order_book.size() * num_runs);
    size_t matches = 0;
    for (size_t i = 0; i < num_runs; ++i) {
        Timer timer;

        timer.start();
        for (auto& timestamp_target : order_book) {
            auto* match = search(prices, timestamp_target.key);
            matches += match != nullptr;
        }
        times[i] = timer.stop();
    }

    /// To avoid optimization removing.
    if (matches == 0) {
        return;
    }

    std::sort(times.begin(), times.end());
    std::cout << fmt::format("Right {}: {}[us]", label, times[0]) << std::endl;
}


void benchmarks::run_different_search_algorithms() {
    size_t num_runs = 3;

    size_t num_prices = 10'000'000;
    size_t num_positions = 10'000'000;

    auto prices = generate_random_price_list(num_prices);
    auto order_book = generate_random_orderbook_list(num_positions, num_prices * PRICE_SAMPLING_RATE);

    std::vector<std::pair<fn, std::string_view>> left_search_algos = {
         {Search::Binary::greater_equal_than<TestEntry>, "Binary"},
         {Search::Exponential::greater_equal_than<TestEntry>, "Exponential"},
         {Search::Interpolation::greater_equal_than<TestEntry>, "Interpolation"}
    };

    std::vector<std::pair<fn, std::string_view>> right_search_algos = {
         {Search::Binary::less_equal_than<TestEntry>, "Binary"},
         {Search::Exponential::less_equal_than<TestEntry>, "Exponential"},
         {Search::Interpolation::less_equal_than<TestEntry>, "Interpolation"}
    };

    std::cout << "Uniform:" << std::endl;
    for (auto& left_search_algo : left_search_algos) {
        benchmark_search_left(
            prices,
            order_book,
            left_search_algo.first,
            left_search_algo.second,
            num_runs);
    }

    for (auto& right_search_algo : right_search_algos) {
        benchmark_search_right(
            prices,
            order_book,
            right_search_algo.first,
            right_search_algo.second,
            num_runs);
    }

    std::cout << "Zipfian 1.5" << std::endl;
    auto zipf_prices = generate_zipfian_price_list(num_prices, 1);
    auto zipf_order_book = generate_zipfian_orderbook_list(num_positions, 1);
    for (auto& left_search_algo : left_search_algos) {
        benchmark_search_left(
            prices,
            zipf_order_book,
            left_search_algo.first,
            left_search_algo.second,
            num_runs);
    }

    for (auto& right_search_algo : right_search_algos) {
        benchmark_search_right(
            zipf_prices,
            order_book,
            right_search_algo.first,
            right_search_algo.second,
            num_runs);
    }
}

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

void benchmark_left_partition_search(Prices& prices, OrderBook& order_book) {
    MultiMapTB<ASOFJoin::LeftEntry> order_book_lookup(order_book.stock_ids, order_book.timestamps);
    tbb::parallel_for_each(order_book_lookup.begin(), order_book_lookup.end(),
            [&](auto& iter) {
        tbb::parallel_sort(iter.second.begin(), iter.second.end());
    });

    size_t matches = 0;
    size_t best = 99999999999;
    for (size_t n = 0; n < 5; ++n) {
        Timer timer;
        timer.start();
        //PerfEventBlock e(prices.size);
        for (size_t i = 0; i < 10; ++i) {
            const auto& stock_id = prices.stock_ids[i];
            if (!order_book_lookup.contains(stock_id)) {
                continue;
            }

            auto& partition_bin = order_book_lookup[stock_id];
            auto timestamp = prices.timestamps[i];
            auto* match= Search::Interpolation::greater_equal_than(
                /* data= */ partition_bin,
                /* target= */ timestamp);

            if (match != nullptr) {
                uint64_t diff = match->timestamp - timestamp;
                match->lock_compare_swap_diffs(diff, i);
                ++matches;
            }
            //break;
        }
        auto dur = timer.stop();
        best = std::min(dur, best);

        //std::cout << "Timer: " << dur << timer.unit() << std::endl;
    }
    std::cout << "Timer: " << best << std::endl;
    std::cout << "LP num matches: " << matches << std::endl;
}


void benchmark_right_partition_search(Prices& prices, OrderBook& order_book) {
    MultiMapTB<ASOFJoin::RightEntry> prices_lookup(prices.stock_ids, prices.timestamps);
    tbb::parallel_for_each(prices_lookup.begin(), prices_lookup.end(),
            [&](auto& iter) {
        tbb::parallel_sort(iter.second.begin(), iter.second.end());
    });

    size_t best = 99999999999999;
    size_t matches = 0;
    for (size_t n = 0; n < 5; ++n) {
        //PerfEventBlock e(order_book.size);
        Timer timer;
        timer.start();
        for (size_t i = 0; i < 10; ++i) {
            auto &stock_id = order_book.stock_ids[i];
            if (!prices_lookup.contains(stock_id)) {
                continue;
            }

            auto &partition_bin = prices_lookup[stock_id];
            auto timestamp = order_book.timestamps[i];
            auto *match = Search::Interpolation::less_equal_than(
                    /* data= */ partition_bin,
                    /* target= */ timestamp);

            if (match != nullptr) {
                ++matches;
            }
        }

        auto dur = timer.stop();
        best = std::min(dur, best);
        //std::cout << "Timer: " << dur << timer.unit() << std::endl;
    }
    std::cout << "Timer: " << best << std::endl;
    std::cout << "RP num matches: " << matches << std::endl;
}

void benchmarks::benchmark_partitioning_search_part() {
    size_t l_r_size = 2'000'000;
    size_t max_timestamp = 50 * l_r_size;
    size_t num_partitions = 1;

    std::vector<double> percentiles = {0, 0.999};

    Prices prices = generate_uniform_prices(l_r_size, max_timestamp, num_partitions);

    for (auto p : percentiles) {
        size_t min_timestamp = p * max_timestamp;
        OrderBook order_book = generate_uniform_orderbook(10000, min_timestamp, max_timestamp, num_partitions);

        benchmark_left_partition_search(prices, order_book);
        benchmark_right_partition_search(prices, order_book);
    }

}
