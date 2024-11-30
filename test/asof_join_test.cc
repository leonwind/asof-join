#include <gtest/gtest.h>

#include "relation.hpp"
#include "asof_join.hpp"

/// The results were manual verified using DuckDB.

TEST(asof_join_baseline, TestSmallDuckDBExample) {
    Prices prices = load_prices("../data/prices_small.csv");
    OrderBook order_book = load_order_book("../data/orderbook_small.csv");
    BaselineASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.values.size(), 4);
    uint64_t appl_sum = 0;
    uint64_t goog_sum = 0;
    for (size_t i = 0; i < join.result.size; ++i) {
        if (join.result.prices_stock_ids[i] == "APPL") { appl_sum += join.result.values[i]; }
        if (join.result.prices_stock_ids[i] == "GOOG") { goog_sum += join.result.values[i]; }
    }
    ASSERT_EQ(appl_sum, 5120);
    ASSERT_EQ(goog_sum, 4461);
    ASSERT_EQ(appl_sum + goog_sum, 9581);
}

TEST(asof_join_sorted_merge_join, TestSmallDuckDBExample) {
    Prices prices = load_prices("../data/prices_small.csv");
    OrderBook order_book = load_order_book("../data/orderbook_small.csv");
    SortingASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.values.size(), 4);
    uint64_t appl_sum = 0;
    uint64_t goog_sum = 0;
    uint64_t total_sum = 0;
    for (size_t i = 0; i < join.result.size; ++i) {
        if (join.result.prices_stock_ids[i] == "APPL") { appl_sum += join.result.values[i]; }
        if (join.result.prices_stock_ids[i] == "GOOG") { goog_sum += join.result.values[i]; }
        total_sum += join.result.values[i];
    }
    ASSERT_EQ(appl_sum, 5120);
    ASSERT_EQ(goog_sum, 4461);
    ASSERT_EQ(total_sum, 9581);
}

TEST(asof_join_sorted_merge_join, TestSmallBTCExample) {
    Prices prices = load_prices("../data/btc_usd_data.csv");
    OrderBook order_book = load_order_book("../data/btc_orderbook_small.csv");
    SortingASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();
    auto& result_values = join.result.values;
    std::sort(result_values.begin(), result_values.end());

    std::vector<uint64_t> correct_values = {
        5244, 9632, 247800, 523980, 4648032, 11277600, 11787330, 13727200, 33081768, 35807400
    };
    ASSERT_EQ(result_values.size(), correct_values.size());
    for (size_t i = 0; i < result_values.size(); ++i) {
        ASSERT_EQ(result_values[i], correct_values[i]) << "Failed at index " << i;
    }
}

TEST(asof_join_sorted_merge_join, TestMediumBTCExample) {
    Prices prices = load_prices("../data/btc_usd_data.csv");
    OrderBook order_book = load_order_book("../data/btc_orderbook_medium.csv");
    SortingASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.values.size(), 10000);
    uint64_t total_value = 0;
    for (auto value : join.result.values) { total_value += value; }
    ASSERT_EQ(total_value, 70580346356);
}

TEST(asof_join_partitioning_left, TestSmallDuckDBExample) {
    Prices prices = load_prices("../data/prices_small.csv", ',', true);
    OrderBook order_book = load_order_book("../data/orderbook_small.csv", ',', true);
    PartitioningLeftASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.values.size(), 4);
    uint64_t appl_sum = 0;
    uint64_t goog_sum = 0;
    uint64_t total_sum = 0;
    for (size_t i = 0; i < join.result.size; ++i) {
        if (join.result.prices_stock_ids[i] == "APPL") { appl_sum += join.result.values[i]; }
        if (join.result.prices_stock_ids[i] == "GOOG") { goog_sum += join.result.values[i]; }
        total_sum += join.result.values[i];
    }
    ASSERT_EQ(appl_sum, 5120);
    ASSERT_EQ(goog_sum, 4461);
    ASSERT_EQ(total_sum, 9581);
}

TEST(asof_join_partitioning_left, TestSmallBTCExample) {
    Prices prices = load_prices("../data/btc_usd_data.csv", ',', true);
    OrderBook order_book = load_order_book("../data/btc_orderbook_small.csv", ',', true);
    PartitioningLeftASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();
    auto& result_values = join.result.values;
    std::sort(result_values.begin(), result_values.end());

    std::vector<uint64_t> correct_values = {
        5244, 9632, 247800, 523980, 4648032, 11277600, 11787330, 13727200, 33081768, 35807400
    };
    ASSERT_EQ(result_values.size(), correct_values.size());
    for (size_t i = 0; i < result_values.size(); ++i) {
        ASSERT_EQ(result_values[i], correct_values[i]) << "Failed at index " << i;
    }
}

TEST(asof_join_partitioning_left, TestMediumBTCExample) {
    Prices prices = load_prices("../data/btc_usd_data.csv", ',', true);
    OrderBook order_book = load_order_book("../data/btc_orderbook_medium.csv", ',', true);
    PartitioningLeftASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.values.size(), 10000);
    uint64_t total_value = 0;
    for (auto value : join.result.values) { total_value += value; }
    ASSERT_EQ(total_value, 70580346356);
}

TEST(asof_join_partitioning_right, TestSmallDuckDBExample) {
    Prices prices = load_prices("../data/prices_small.csv", ',', true);
    OrderBook order_book = load_order_book("../data/orderbook_small.csv", ',', true);
    PartitioningRightASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.values.size(), 4);
    uint64_t appl_sum = 0;
    uint64_t goog_sum = 0;
    uint64_t total_sum = 0;
    for (size_t i = 0; i < join.result.size; ++i) {
        if (join.result.prices_stock_ids[i] == "APPL") { appl_sum += join.result.values[i]; }
        if (join.result.prices_stock_ids[i] == "GOOG") { goog_sum += join.result.values[i]; }
        total_sum += join.result.values[i];
    }
    ASSERT_EQ(appl_sum, 5120);
    ASSERT_EQ(goog_sum, 4461);
    ASSERT_EQ(total_sum, 9581);
}

TEST(asof_join_partitioning_right, TestSmallBTCExample) {
    Prices prices = load_prices("../data/btc_usd_data.csv", ',', true);
    OrderBook order_book = load_order_book("../data/btc_orderbook_small.csv", ',', true);
    PartitioningRightASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

   join.join();
    auto& result_values = join.result.values;
    std::sort(result_values.begin(), result_values.end());

    std::vector<uint64_t> correct_values = {
        5244, 9632, 247800, 523980, 4648032, 11277600, 11787330, 13727200, 33081768, 35807400
    };
    ASSERT_EQ(result_values.size(), correct_values.size());
    for (size_t i = 0; i < result_values.size(); ++i) {
        ASSERT_EQ(result_values[i], correct_values[i]) << "Failed at index " << i;
    }
}

TEST(asof_join_partitioning_right, TestMediumBTCExample) {
    Prices prices = load_prices("../data/btc_usd_data.csv", ',', true);
    OrderBook order_book = load_order_book("../data/btc_orderbook_medium.csv", ',', true);
    PartitioningRightASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.values.size(), 10000);
    uint64_t total_value = 0;
    for (auto value : join.result.values) { total_value += value; }
    ASSERT_EQ(total_value, 70580346356);
}

TEST(asof_join_partitioning_sort, TestSmallDuckDBExample) {
    Prices prices = load_prices("../data/prices_small.csv", ',', true);
    OrderBook order_book = load_order_book("../data/orderbook_small.csv", ',', true);
    PartitioningSortedMergeJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.values.size(), 4);
    uint64_t appl_sum = 0;
    uint64_t goog_sum = 0;
    uint64_t total_sum = 0;
    for (size_t i = 0; i < join.result.size; ++i) {
        if (join.result.prices_stock_ids[i] == "APPL") { appl_sum += join.result.values[i]; }
        if (join.result.prices_stock_ids[i] == "GOOG") { goog_sum += join.result.values[i]; }
        total_sum += join.result.values[i];
    }
    ASSERT_EQ(appl_sum, 5120);
    ASSERT_EQ(goog_sum, 4461);
    ASSERT_EQ(total_sum, 9581);
}

TEST(asof_join_partitioning_sort, TestSmallBTCExample) {
    Prices prices = load_prices("../data/btc_usd_data.csv", ',', true);
    OrderBook order_book = load_order_book("../data/btc_orderbook_small.csv", ',', true);
    PartitioningSortedMergeJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();
    auto& result_values = join.result.values;
    std::sort(result_values.begin(), result_values.end());

    std::vector<uint64_t> correct_values = {
        5244, 9632, 247800, 523980, 4648032, 11277600, 11787330, 13727200, 33081768, 35807400
    };
    ASSERT_EQ(result_values.size(), correct_values.size());
    for (size_t i = 0; i < result_values.size(); ++i) {
        ASSERT_EQ(result_values[i], correct_values[i]) << "Failed at index " << i;
    }
}

TEST(asof_join_partitioning_sort, TestMediumBTCExample) {
    Prices prices = load_prices("../data/btc_usd_data.csv", ',', true);
    OrderBook order_book = load_order_book("../data/btc_orderbook_medium.csv", ',', true);
    PartitioningSortedMergeJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.values.size(), 10000);
    uint64_t total_value = 0;
    for (auto value : join.result.values) { total_value += value; }
    ASSERT_EQ(total_value, 70580346356);
}

TEST(asof_join_left_partitioning_btree, TestSmallDuckDBExample) {
    Prices prices = load_prices("../data/prices_small.csv", ',', true);
    OrderBook order_book = load_order_book("../data/orderbook_small.csv", ',', true);
    PartitioningLeftBTreeASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.values.size(), 4);
    uint64_t appl_sum = 0;
    uint64_t goog_sum = 0;
    uint64_t total_sum = 0;
    for (size_t i = 0; i < join.result.size; ++i) {
        if (join.result.prices_stock_ids[i] == "APPL") { appl_sum += join.result.values[i]; }
        if (join.result.prices_stock_ids[i] == "GOOG") { goog_sum += join.result.values[i]; }
        total_sum += join.result.values[i];
    }
    ASSERT_EQ(appl_sum, 5120);
    ASSERT_EQ(goog_sum, 4461);
    ASSERT_EQ(total_sum, 9581);
}

TEST(asof_join_left_partitioning_btree, TestSmallBTCExample) {
    Prices prices = load_prices("../data/btc_usd_data.csv", ',', true);
    OrderBook order_book = load_order_book("../data/btc_orderbook_small.csv", ',', true);
    PartitioningLeftBTreeASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();
    auto& result_values = join.result.values;
    std::sort(result_values.begin(), result_values.end());

    std::vector<uint64_t> correct_values = {
        5244, 9632, 247800, 523980, 4648032, 11277600, 11787330, 13727200, 33081768, 35807400
    };
    ASSERT_EQ(result_values.size(), correct_values.size());
    for (size_t i = 0; i < result_values.size(); ++i) {
        ASSERT_EQ(result_values[i], correct_values[i]) << "Failed at index " << i;
    }
}

TEST(asof_join_left_partitioning_btree, TestMediumBTCExample) {
    Prices prices = load_prices("../data/btc_usd_data.csv", ',', true);
    OrderBook order_book = load_order_book("../data/btc_orderbook_medium.csv", ',', true);
    PartitioningLeftBTreeASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.values.size(), 10000);
    uint64_t total_value = 0;
    for (auto value : join.result.values) { total_value += value; }
    ASSERT_EQ(total_value, 70580346356);
}

TEST(asof_join_right_partitioning_btree, TestSmallDuckDBExample) {
    Prices prices = load_prices("../data/prices_small.csv", ',', true);
    OrderBook order_book = load_order_book("../data/orderbook_small.csv", ',', true);
    PartitioningRightBTreeASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.values.size(), 4);
    uint64_t appl_sum = 0;
    uint64_t goog_sum = 0;
    uint64_t total_sum = 0;
    for (size_t i = 0; i < join.result.size; ++i) {
        if (join.result.prices_stock_ids[i] == "APPL") { appl_sum += join.result.values[i]; }
        if (join.result.prices_stock_ids[i] == "GOOG") { goog_sum += join.result.values[i]; }
        total_sum += join.result.values[i];
    }
    ASSERT_EQ(appl_sum, 5120);
    ASSERT_EQ(goog_sum, 4461);
    ASSERT_EQ(total_sum, 9581);
}

TEST(asof_join_right_partitioning_btree, TestSmallBTCExample) {
    Prices prices = load_prices("../data/btc_usd_data.csv", ',', true);
    OrderBook order_book = load_order_book("../data/btc_orderbook_small.csv", ',', true);
    PartitioningRightBTreeASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();
    auto& result_values = join.result.values;
    std::sort(result_values.begin(), result_values.end());

    std::vector<uint64_t> correct_values = {
        5244, 9632, 247800, 523980, 4648032, 11277600, 11787330, 13727200, 33081768, 35807400
    };
    ASSERT_EQ(result_values.size(), correct_values.size());
    for (size_t i = 0; i < result_values.size(); ++i) {
        ASSERT_EQ(result_values[i], correct_values[i]) << "Failed at index " << i;
    }
}

TEST(asof_join_right_partitioning_btree, TestMediumBTCExample) {
    Prices prices = load_prices("../data/btc_usd_data.csv", ',', true);
    OrderBook order_book = load_order_book("../data/btc_orderbook_medium.csv", ',', true);
    PartitioningRightBTreeASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.values.size(), 10000);
    uint64_t total_value = 0;
    for (auto value : join.result.values) { total_value += value; }
    ASSERT_EQ(total_value, 70580346356);
}

TEST(asof_join_left_partitioning_split_bs, TestSmallDuckDBExample) {
    Prices prices = load_prices("../data/prices_small.csv", ',', true);
    OrderBook order_book = load_order_book("../data/orderbook_small.csv", ',', true);
    PartitioningLeftSplitBinarySearchASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.values.size(), 4);
    uint64_t appl_sum = 0;
    uint64_t goog_sum = 0;
    uint64_t total_sum = 0;
    for (size_t i = 0; i < join.result.size; ++i) {
        if (join.result.prices_stock_ids[i] == "APPL") { appl_sum += join.result.values[i]; }
        if (join.result.prices_stock_ids[i] == "GOOG") { goog_sum += join.result.values[i]; }
        total_sum += join.result.values[i];
    }
    ASSERT_EQ(appl_sum, 5120);
    ASSERT_EQ(goog_sum, 4461);
    ASSERT_EQ(total_sum, 9581);
}

TEST(asof_join_left_partitioning_split_bs, TestSmallBTCExample) {
    Prices prices = load_prices("../data/btc_usd_data.csv", ',', true);
    OrderBook order_book = load_order_book("../data/btc_orderbook_small.csv", ',', true);
    PartitioningLeftSplitBinarySearchASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();
    auto& result_values = join.result.values;
    std::sort(result_values.begin(), result_values.end());

    std::vector<uint64_t> correct_values = {
        5244, 9632, 247800, 523980, 4648032, 11277600, 11787330, 13727200, 33081768, 35807400
    };
    ASSERT_EQ(result_values.size(), correct_values.size());
    for (size_t i = 0; i < result_values.size(); ++i) {
        ASSERT_EQ(result_values[i], correct_values[i]) << "Failed at index " << i;
    }
}

TEST(asof_join_left_partitioning_split_bs, TestMediumBTCExample) {
    Prices prices = load_prices("../data/btc_usd_data.csv", ',', true);
    OrderBook order_book = load_order_book("../data/btc_orderbook_medium.csv", ',', true);
    PartitioningLeftSplitBinarySearchASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.values.size(), 10000);
    uint64_t total_value = 0;
    for (auto value : join.result.values) { total_value += value; }
    ASSERT_EQ(total_value, 70580346356);
}


TEST(asof_join_right_partitioning_split_bs, TestSmallDuckDBExample) {
    Prices prices = load_prices("../data/prices_small.csv", ',', true);
    OrderBook order_book = load_order_book("../data/orderbook_small.csv", ',', true);
    PartitioningRightSplitBinarySearchASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.values.size(), 4);
    uint64_t appl_sum = 0;
    uint64_t goog_sum = 0;
    uint64_t total_sum = 0;
    for (size_t i = 0; i < join.result.size; ++i) {
        if (join.result.prices_stock_ids[i] == "APPL") { appl_sum += join.result.values[i]; }
        if (join.result.prices_stock_ids[i] == "GOOG") { goog_sum += join.result.values[i]; }
        total_sum += join.result.values[i];
    }
    ASSERT_EQ(appl_sum, 5120);
    ASSERT_EQ(goog_sum, 4461);
    ASSERT_EQ(total_sum, 9581);
}

TEST(asof_join_right_partitioning_split_bs, TestSmallBTCExample) {
    Prices prices = load_prices("../data/btc_usd_data.csv", ',', true);
    OrderBook order_book = load_order_book("../data/btc_orderbook_small.csv", ',', true);
    PartitioningRightSplitBinarySearchASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();
    auto& result_values = join.result.values;
    std::sort(result_values.begin(), result_values.end());

    std::vector<uint64_t> correct_values = {
        5244, 9632, 247800, 523980, 4648032, 11277600, 11787330, 13727200, 33081768, 35807400
    };
    ASSERT_EQ(result_values.size(), correct_values.size());
    for (size_t i = 0; i < result_values.size(); ++i) {
        ASSERT_EQ(result_values[i], correct_values[i]) << "Failed at index " << i;
    }
}

TEST(asof_join_right_partitioning_split_bs, TestMediumBTCExample) {
    Prices prices = load_prices("../data/btc_usd_data.csv", ',', true);
    OrderBook order_book = load_order_book("../data/btc_orderbook_medium.csv", ',', true);
    PartitioningRightSplitBinarySearchASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.values.size(), 10000);
    uint64_t total_value = 0;
    for (auto value : join.result.values) { total_value += value; }
    ASSERT_EQ(total_value, 70580346356);
}
