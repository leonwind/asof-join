#include <iostream>
#include <vector>
#include <fmt/format.h>
#include <thread>
#include <functional>

#include "tbb/parallel_sort.h"
#include "tbb/parallel_for.h"
#include "tbb/parallel_for_each.h"

#include "uniform_gen.hpp"
#include "searches.hpp"
#include "relation.hpp"
#include "benchmark.hpp"
#include "parallel_multi_map.hpp"


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

    std::vector<TestEntry> generate_random_prices(size_t num_prices) {
        std::vector<TestEntry> data;

        for (size_t i = 0; i < num_prices; ++i) {
            data.emplace_back(i * PRICE_SAMPLING_RATE);
        }

        return data;
    }

    std::vector<TestEntry> generate_random_orderbook(size_t num_orders, size_t max_timestamp) {
        std::vector<TestEntry> data;

        for (size_t i = 0; i < num_orders; ++i) {
            data.emplace_back(uniform::gen_int(max_timestamp));
        }

        tbb::parallel_sort(data.begin(), data.end());
        return data;
    }
} // namespace

void benchmark_search_left(std::vector<TestEntry>& prices, std::vector<TestEntry>& order_book,
                           const fn& search, std::string_view label, size_t num_runs) {
    std::vector<uint64_t> times(num_runs);

    size_t matches = 0;
    PerfEventBlock e(prices.size() * num_runs);
    //search(order_book, 1'000'001);
    //return;
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
    //if (matches == 0) {
    //    return;
    //}
    std::cout << "Num matches: " << matches << std::endl;

    std::sort(times.begin(), times.end());
    std::cout << fmt::format("Left {}: {}[us]", label, times[0]) << std::endl;
}


void benchmark_search_right(std::vector<TestEntry>& prices, std::vector<TestEntry>& order_book,
                            const fn& search, std::string_view label, size_t num_runs) {
    std::vector<uint64_t> times(num_runs);

    PerfEventBlock e(order_book.size() * num_runs);
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
    //if (matches == 0) {
    //    return;
    //}
    std::cout << "Num matches: " << matches << std::endl;

    std::sort(times.begin(), times.end());
    std::cout << fmt::format("Right {}: {}[us]", label, times[0]) << std::endl;
}


void benchmarks::run_different_search_algorithms() {
    size_t num_runs = 3;

    size_t num_prices = 10'000'000;
    size_t num_positions = 10'000'000;

    auto prices = generate_random_prices(num_prices);
    auto order_book = generate_random_orderbook(num_positions, num_prices * PRICE_SAMPLING_RATE);

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
}
