#include <gtest/gtest.h>
#include <iostream>
#include <vector>

#include "asof_join.hpp"
#include "parallel_multi_map.hpp"
#include "fmt/format.h"
#include "tbb/global_control.h"

namespace {
    struct TestEntry: ASOFJoin::JoinEntry {
        uint64_t timestamp;
        size_t idx;

        TestEntry(): timestamp(-1), idx(-1) {};

        TestEntry(uint64_t timestamp, size_t idx): timestamp(timestamp), idx(idx) {}

        [[nodiscard]] inline uint64_t get_key() const override {
            return timestamp;
        }

        std::string str() {
            return fmt::format("[{}, {}]", timestamp, idx);
        }
    };

    std::string generate_key(size_t i) { return fmt::format("s-{}", i); }

    std::pair<std::vector<std::string>, std::vector<size_t>> generate_data(
            size_t num_keys, size_t num_entries_per_key) {
        std::vector<std::string> keys;
        std::vector<size_t> values;
        for (size_t i = 0; i < num_keys * num_entries_per_key; ++i) {
            std::string key = fmt::format("s-{}", i % num_keys);
            keys.push_back(key);
            values.push_back(i);
        }

        return {keys, values};
    }
} // namespace


TEST(multimap, SingleInsert) {
    std::vector<std::string> keys{"key0"};
    std::vector<uint64_t> values{0};

    MultiMapTB<TestEntry> multi_map(keys, values, /* num_partitions= */ 2);

    auto& data = multi_map["key0"];
    ASSERT_EQ(data.size(), 1);
    ASSERT_EQ(data[0].timestamp, 0);
    ASSERT_EQ(data[0].idx, 0);
}

TEST(multimap, SmallInsertLookup) {
    size_t num_keys = 5;
    size_t num_entries_per_key = 10000;
    auto [keys, values] = generate_data(
        num_keys,
        num_entries_per_key);

    MultiMapTB<TestEntry> multi_map(keys, values);

    for (size_t i = 0; i < num_keys; ++i) {
        auto key = generate_key(i);
        auto data = multi_map[key];//.copy_tuples();
        ASSERT_EQ(data.size(), num_entries_per_key);

        std::sort(data.begin(), data.end());
        for (size_t j = 0; j < num_entries_per_key; ++j) {
            std::string error_msg = fmt::format("Failed at key {}, j={}", key, j);
            size_t correct_value = i + j * num_keys;
            ASSERT_EQ(data[j].timestamp, correct_value) << error_msg;
            ASSERT_EQ(data[j].idx, correct_value) << error_msg;
        }
    }
}

TEST(multimap, MoreKeysThanPartitions) {
    size_t num_keys = 4096;
    size_t num_entries_per_key = 1000;
    auto [keys, values] = generate_data(
        num_keys,
        num_entries_per_key);

    MultiMapTB<TestEntry> multi_map(keys, values);
    for (size_t i = 0; i < num_keys; ++i) {
        auto key = generate_key(i);
        auto data = multi_map[key]; //.copy_tuples();
        ASSERT_EQ(data.size(), num_entries_per_key);
        std::sort(data.begin(), data.end());
        for (size_t j = 0; j < num_entries_per_key; ++j) {
            size_t correct_value = /* offset= */ i + j * num_keys;
            ASSERT_EQ(data[j].timestamp, correct_value) << fmt::format("Failed at key {}, j={}", key, j);
            ASSERT_EQ(data[j].idx, correct_value) << fmt::format("Failed at key {}, j={}", key, j);
        }
    }
}

TEST(multimap, Benchmark) {
    return;

    /// Benchmark for thread scalability
    size_t num_keys = 128;
    size_t num_entries_per_key = 100000;
    auto [keys, values] = generate_data(
        num_keys,
        num_entries_per_key);

    const size_t num_cores = std::thread::hardware_concurrency();

    double single_dur = 1;

    for (size_t threads = 1; threads <= num_cores; ++threads) {
        tbb::global_control control(tbb::global_control::max_allowed_parallelism, threads);
        Timer<milliseconds> timer;
        timer.start();
        MultiMapTB<TestEntry> multi_map(keys, values);
        auto dur = timer.stop();
        auto factor = single_dur / dur;
        std::cout << "Num threads: " << threads << ", dur: " << dur << timer.unit() << std::endl;
        std::cout << "Speedup: " <<  factor << std::endl;
        std::cout << "\n###\n" << std::endl;
        if (threads == 1) {
            single_dur = dur;
        }
        for (size_t i = 0; i < num_keys; ++i) {
            auto key = generate_key(i);
            auto data = multi_map[key]; //.copy_tuples();
            ASSERT_EQ(data.size(), num_entries_per_key);
            std::sort(data.begin(), data.end());
            for (size_t j = 0; j < num_entries_per_key; ++j) {
                size_t correct_value = /* offset= */ i + j * num_keys;
                ASSERT_EQ(data[j].timestamp, correct_value) << fmt::format("Failed at key {}, j={}", key, j);
                ASSERT_EQ(data[j].idx, correct_value) << fmt::format("Failed at key {}, j={}", key, j);
            }
        }
    }

}
