#include <gtest/gtest.h>
#include <iostream>
#include <vector>
#include <functional>

#include "fmt/format.h"

#include "timer.hpp"
#include "searches.hpp"

using namespace Search;

namespace {
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

    std::vector<TestEntry> generate_data(size_t n, size_t data_gap = 1, size_t offset = 0) {
        std::vector<TestEntry> data;

        size_t val = offset;
        for (size_t i = 0; i < n; ++i) {
            data.emplace_back(val);
            val += data_gap;
        }

        return data;
    }

    std::string error_msg(size_t idx) {
        return fmt::format("For index {}", idx);
    }

    using SearchFn = std::function<TestEntry*(std::vector<TestEntry>&, uint64_t)>;

    void test_search_less_than_small(const SearchFn& search_less_equal_than) {
        size_t n = 5;
        size_t offset = 10;
        auto data = generate_data(/* n= */ n, /* data_gap= */ 1, /* offset= */ 10);

        for (size_t i = 0; i < n + offset + 10; ++i) {
            auto *res = search_less_equal_than(data, i);
            if (i < offset) {
                ASSERT_TRUE(res == nullptr) << error_msg(i) << "No value should be found";
            } else if (i < n + offset){
                ASSERT_TRUE(res != nullptr) << error_msg(i) << "Actual value should be found";
                ASSERT_EQ(res->get_key(), i) << error_msg(i) << "Actual value should be found";
            } else {
                ASSERT_TRUE(res != nullptr) << error_msg(i);
                ASSERT_EQ(res->get_key(), data[n - 1].get_key())
                    << error_msg(i)
                    << "Max value should be found";
            }
        }
    }

    void test_search_greater_than_small(const SearchFn& search_greater_equal_than) {
        size_t n = 5;
        size_t offset = 10;
        auto data = generate_data(/* n= */ n, /* data_gap= */ 1, /* offset= */ 10);

        for (size_t i = 0; i < n + offset + 10; ++i) {
            auto *res = search_greater_equal_than(data, i);
            if (i < offset) {
                ASSERT_TRUE(res != nullptr) << error_msg(i);
                ASSERT_EQ(res->get_key(), data[0].get_key()) << error_msg(i);
            } else if (i < n + offset) {
                ASSERT_TRUE(res != nullptr) << error_msg(i);
                ASSERT_EQ(res->get_key(), i) << error_msg(i);
            } else {
                ASSERT_TRUE(res == nullptr) << error_msg(i);
            }
        }
    }
} // namespace

TEST(binary_search, SearchLessThanSmall) {
    test_search_less_than_small(Binary::less_equal_than<TestEntry>);
}

TEST(binary_search, SearchGreaterThanSmall) {
    test_search_greater_than_small(Binary::greater_equal_than<TestEntry>);
}

TEST(exponential_search, SearchLessThanSmall) {
    test_search_less_than_small(Exponential::less_equal_than<TestEntry>);
}

TEST(exponential_search, SearchGreaterThanSmall) {
    test_search_greater_than_small(Exponential::greater_equal_than<TestEntry>);
}

TEST(interpolation_search, SearchLessThanSmall) {
    test_search_less_than_small(Interpolation::less_equal_than<TestEntry>);
}

TEST(interpolation_search, SearchGreaterThanSmall) {
    test_search_greater_than_small(Interpolation::greater_equal_than<TestEntry>);
}
