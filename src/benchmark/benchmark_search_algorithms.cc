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

    std::vector<TestEntry> generate_uniform_price_list(size_t num_prices) {
        std::vector<TestEntry> data;

        for (size_t i = 0; i < num_prices; ++i) {
            data.emplace_back(uniform::gen_int(num_prices * PRICE_SAMPLING_RATE));
        }

        tbb::parallel_sort(data.begin(), data.end());
        return data;
    }

    std::vector<TestEntry> generate_zipf_price_list(size_t num_prices, double zipf_skew) {
        std::mt19937 rng(std::random_device{}());
        Zipf zipf_gen(num_prices, zipf_skew);
        std::vector<TestEntry> data;

        for (size_t i = 0; i < num_prices; ++i) {
            data.emplace_back(zipf_gen(rng));
        }

        tbb::parallel_sort(data.begin(), data.end());
        return data;
    }

    std::vector<TestEntry> generate_normal_orderbook_list(size_t num_orders) {
        std::mt19937 rng(std::random_device{}());
        auto mean = num_orders * PRICE_SAMPLING_RATE * 3 / 4;
        auto variance = num_orders * PRICE_SAMPLING_RATE / 8;

        std::normal_distribution<double> normal_gen(mean, variance);
        auto random_normal_int = [&normal_gen, &rng] {
            return static_cast<size_t>(std::llround(normal_gen(rng)));
        };

        std::vector<TestEntry> data;
        for (size_t i = 0; i < num_orders; ++i) {
            data.emplace_back(random_normal_int());
        }

        tbb::parallel_sort(data.begin(), data.end());
        return data;
    }

    std::vector<TestEntry> generate_uniform_orderbook_list(size_t num_orders, size_t max_timestamp) {
        std::vector<TestEntry> data;
        for (size_t i = 0; i < num_orders; ++i) {
            data.emplace_back(uniform::gen_int(max_timestamp));
        }

        tbb::parallel_sort(data.begin(), data.end());
        return data;
    }
} // namespace


void benchmark_search(std::vector<TestEntry>& data, std::vector<TestEntry>& targets,
                      const fn& search, std::string_view label, size_t num_runs) {
    std::vector<uint64_t> times(num_runs);

    //PerfEventBlock e(prices.size() * num_runs);
    size_t matches = 0;
    for (size_t i = 0; i < num_runs; ++i) {
        Timer timer;

        timer.start();
        for (auto& target : targets) {
            auto* match = search(data, target.key);
            matches += match != nullptr;
        }
        times[i] = timer.stop();
    }

    /// To avoid optimization removing.
    if (matches == 0) {
        return;
    }

    std::sort(times.begin(), times.end());
    std::cout << fmt::format("{}: {}[us]", label, times[0]) << std::endl;
}


void benchmarks::run_different_search_algorithms() {
    size_t num_runs = 3;

    size_t data_size = 100'000'000;
    size_t num_targets = 10'000'000;

    std::vector<std::pair<std::vector<TestEntry>, std::string_view>> data_dists = {
            {generate_uniform_price_list(data_size), "Uniform"},
            {generate_zipf_price_list(data_size, 1), "Zipf 1"},
            {generate_normal_orderbook_list(data_size), "Normal"},
    };

    auto uniform_targets =
            generate_uniform_orderbook_list(num_targets, data_size * PRICE_SAMPLING_RATE);

    std::vector<std::pair<fn, std::string_view>> search_algos = {
         {Search::Binary::less_equal_than<TestEntry>, "Binary LE"},
         {Search::Exponential::less_equal_than<TestEntry>, "Exponential LE"},
         {Search::Interpolation::less_equal_than<TestEntry>, "Interpolation LE"},
         {Search::Binary::greater_equal_than<TestEntry>, "Binary GE"},
         {Search::Exponential::greater_equal_than<TestEntry>, "Exponential GE"},
         {Search::Interpolation::greater_equal_than<TestEntry>, "Interpolation GE"}
    };

    for (auto& [data, dist_label] : data_dists) {
        std::cout << dist_label << ":" << std::endl;
        for (auto &[search_fn, search_label]: search_algos) {
            benchmark_search(
                data,
                uniform_targets,
                search_fn,
                search_label,
                num_runs);
        }
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
