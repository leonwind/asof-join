#include <gtest/gtest.h>
#include <iostream>
#include <vector>

#include "fmt/format.h"

#include "btree.hpp"
#include "asof_join.hpp"

namespace {
    struct TestEntry: ASOFJoin::JoinEntry {
        uint64_t key;
        uint64_t value;

        TestEntry(uint64_t key, uint64_t value): key(key), value(value) {}

        [[nodiscard]] inline uint64_t getKey() const override {
            return key;
        }

        std::string str() {
            return fmt::format("[{}, {}]", key, value);
        }
    };
} // namespace

TEST(btree, SingleInsertLookup) {
    auto entry = TestEntry(1, 100);
    auto data = std::vector<TestEntry>{entry};

    auto tree = Btree<TestEntry>(data);
    auto result = tree.find_less_equal_than(entry.getKey() + 1);

    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value().key, entry.key);
    ASSERT_EQ(result.value().value, entry.value);
}

TEST(btree, InsertMediumLookupLessEqual) {
    size_t offset = 10;
    size_t num_entries = 33;
    auto data = std::vector<TestEntry>{};
    for (size_t i = 0; i < num_entries; ++i) {
        data.emplace_back(i + offset, i);
    }

    auto tree = Btree<TestEntry>(data);

    for (size_t i = 0; i < num_entries; ++i) {
        auto res = tree.find_less_equal_than(i + offset);
        ASSERT_TRUE(res.has_value()) << "Key " << i + offset << " does not exist.";
        ASSERT_EQ(res.value().key, i + offset);
        ASSERT_EQ(res.value().value, i);
    }

    for (size_t i = 0; i < offset; ++i) {
        auto res = tree.find_less_equal_than(i);
        ASSERT_FALSE(res.has_value()) << "Key " << i << " does exist.";
    }
}

TEST(btree, InsertLargeLookupLessEqual) {
    size_t offset = 10;
    size_t num_entries = 1000000;
    auto data = std::vector<TestEntry>{};
    for (size_t i = 0; i < num_entries; ++i) {
        data.emplace_back(i + offset, i);
    }

    auto tree = Btree<TestEntry>(data);
    for (size_t i = 0; i < num_entries; ++i) {
        auto res = tree.find_less_equal_than(i + offset);
        ASSERT_TRUE(res.has_value()) << "Key " << i + offset << " does not exist.";
        ASSERT_EQ(res.value().key, i + offset);
        ASSERT_EQ(res.value().value, i);
    }

    for (size_t i = 0; i < offset; ++i) {
        auto res = tree.find_less_equal_than(i);
        ASSERT_FALSE(res.has_value()) << "Key " << i << " does exist.";
    }
}

TEST(btree, InsertLargeLookupGreaterEqual) {
    size_t num_entries = 1000000;
    auto data = std::vector<TestEntry>{};
    for (size_t i = 0; i < num_entries; ++i) {
        data.emplace_back(i, i);
    }

    auto tree = Btree<TestEntry>(data);
    for (size_t i = 0; i < num_entries; ++i) {
        auto res = tree.find_greater_equal_than(i);
        ASSERT_TRUE(res.has_value()) << "Key " << i << " does not exist.";
        ASSERT_EQ(res.value().key, i);
        ASSERT_EQ(res.value().value, i);
    }

    for (size_t i = num_entries; i < num_entries + 10; ++i) {
        auto res = tree.find_greater_equal_than(i);
        ASSERT_FALSE(res.has_value()) << "Key " << i << " does exist.";
    }
}
