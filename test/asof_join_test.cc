#include <gtest/gtest.h>

#include "relation.hpp"
#include "asof_join.hpp"

/// The results were manual verified using DuckDB.

TEST(asof_join_baseline, TestSmallDuckDBExample) {
    Prices prices = load_prices("../test/data/prices_small.csv");
    OrderBook order_book = load_order_book("../test/data/orderbook_small.csv");
    BaselineASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.size, 4);
    ASSERT_EQ(join.result.value_sum, 9581);
}

TEST(asof_join_sorted_merge_join, TestSmallDuckDBExample) {
    Prices prices = load_prices("../test/data/prices_small.csv");
    OrderBook order_book = load_order_book("../test/data/orderbook_small.csv");
    SortingASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.size, 4);
    ASSERT_EQ(join.result.value_sum, 9581);
}

TEST(asof_join_sorted_merge_join, TestSmallBTCExample) {
    Prices prices = load_prices("../test/data/btc_usd_data.csv");
    OrderBook order_book = load_order_book("../test/data/btc_orderbook_small.csv");
    SortingASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();
    auto result_values = join.result.collect_values();
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
    Prices prices = load_prices("../test/data/btc_usd_data.csv");
    OrderBook order_book = load_order_book("../test/data/btc_orderbook_medium.csv");
    SortingASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.size, 10000);
    ASSERT_EQ(join.result.value_sum, 70580346356);
}

TEST(asof_join_partitioning_right, TestSmallDuckDBExample) {
    Prices prices = load_prices("../test/data/prices_small.csv", ',', true);
    OrderBook order_book = load_order_book("../test/data/orderbook_small.csv", ',', true);
    PartitioningRightASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.size, 4);
    ASSERT_EQ(join.result.value_sum, 9581);
}

TEST(asof_join_partitioning_right, TestSmallBTCExample) {
    Prices prices = load_prices("../test/data/btc_usd_data.csv", ',', true);
    OrderBook order_book = load_order_book("../test/data/btc_orderbook_small.csv", ',', true);
    PartitioningRightASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();
    auto result_values = join.result.collect_values();
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
    Prices prices = load_prices("../test/data/btc_usd_data.csv", ',', true);
    OrderBook order_book = load_order_book("../test/data/btc_orderbook_medium.csv", ',', true);
    PartitioningRightASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.size, 10000);
    ASSERT_EQ(join.result.value_sum, 70580346356);
}

TEST(asof_join_partitioning_right, TestSmallUniformExample) {
    Prices prices = load_prices("../test/data/uniform_small_prices.csv", ',', true);
    OrderBook order_book = load_order_book("../test/data/uniform_small_positions.csv", ',', true);
    PartitioningRightASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.size, 10000);
    ASSERT_EQ(join.result.value_sum, 24168000);
}

TEST(asof_join_partitioning_right, TestSmallZipfExample) {
    Prices prices = load_prices("../test/data/zipf_small_prices.csv", ',', true);
    OrderBook order_book = load_order_book("../test/data/zipf_small_positions.csv", ',', true);
    PartitioningRightASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.size, 1024);
    ASSERT_EQ(join.result.value_sum, 2532396);
}

TEST(asof_join_partitioning_left, TestSmallDuckDBExample) {
    Prices prices = load_prices("../test/data/prices_small.csv", ',', true);
    OrderBook order_book = load_order_book("../test/data/orderbook_small.csv", ',', true);
    PartitioningLeftASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.size, 4);
    ASSERT_EQ(join.result.value_sum, 9581);
}

TEST(asof_join_partitioning_left, TestSmallBTCExample) {
    Prices prices = load_prices("../test/data/btc_usd_data.csv", ',', true);
    OrderBook order_book = load_order_book("../test/data/btc_orderbook_small.csv", ',', true);
    PartitioningLeftASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();
    auto result_values = join.result.collect_values();
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
    Prices prices = load_prices("../test/data/btc_usd_data.csv", ',', true);
    OrderBook order_book = load_order_book("../test/data/btc_orderbook_medium.csv", ',', true);
    PartitioningLeftASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.size, 10000);
    ASSERT_EQ(join.result.value_sum, 70580346356);
}

TEST(asof_join_partitioning_left, TestSmallUniformExample) {
    Prices prices = load_prices("../test/data/uniform_small_prices.csv", ',', true);
    OrderBook order_book = load_order_book("../test/data/uniform_small_positions.csv", ',', true);
    PartitioningLeftASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.size, 10000);
    ASSERT_EQ(join.result.value_sum, 24168000);
}

TEST(asof_join_partitioning_left, TestSmallZipfExample) {
    Prices prices = load_prices("../test/data/zipf_small_prices.csv", ',', true);
    OrderBook order_book = load_order_book("../test/data/zipf_small_positions.csv", ',', true);
    PartitioningLeftASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.size, 1024);
    ASSERT_EQ(join.result.value_sum, 2532396);
}

TEST(asof_join_partitioning_sort, TestSmallDuckDBExample) {
    Prices prices = load_prices("../test/data/prices_small.csv", ',', true);
    OrderBook order_book = load_order_book("../test/data/orderbook_small.csv", ',', true);
    PartitioningSortedMergeJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.size, 4);
    ASSERT_EQ(join.result.value_sum, 9581);
}

TEST(asof_join_partitioning_sort, TestSmallBTCExample) {
    Prices prices = load_prices("../test/data/btc_usd_data.csv", ',', true);
    OrderBook order_book = load_order_book("../test/data/btc_orderbook_small.csv", ',', true);
    PartitioningSortedMergeJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();
    auto result_values = join.result.collect_values();
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
    Prices prices = load_prices("../test/data/btc_usd_data.csv", ',', true);
    OrderBook order_book = load_order_book("../test/data/btc_orderbook_medium.csv", ',', true);
    PartitioningSortedMergeJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.size, 10000);
    ASSERT_EQ(join.result.value_sum, 70580346356);
}

TEST(asof_join_partitioning_sort, TestSmallUniformExample) {
    Prices prices = load_prices("../test/data/uniform_small_prices.csv", ',', true);
    OrderBook order_book = load_order_book("../test/data/uniform_small_positions.csv", ',', true);
    PartitioningSortedMergeJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.size, 10000);
    ASSERT_EQ(join.result.value_sum, 24168000);
}

TEST(asof_join_partitioning_sort, TestSmallZipfExample) {
    Prices prices = load_prices("../test/data/zipf_small_prices.csv", ',', true);
    OrderBook order_book = load_order_book("../test/data/zipf_small_positions.csv", ',', true);
    PartitioningSortedMergeJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.size, 1024);
    ASSERT_EQ(join.result.value_sum, 2532396);
}

TEST(asof_join_right_partitioning_btree, TestSmallDuckDBExample) {
    Prices prices = load_prices("../test/data/prices_small.csv", ',', true);
    OrderBook order_book = load_order_book("../test/data/orderbook_small.csv", ',', true);
    PartitioningRightBTreeASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.size, 4);
    ASSERT_EQ(join.result.value_sum, 9581);
}

TEST(asof_join_right_partitioning_btree, TestSmallBTCExample) {
    Prices prices = load_prices("../test/data/btc_usd_data.csv", ',', true);
    OrderBook order_book = load_order_book("../test/data/btc_orderbook_small.csv", ',', true);
    PartitioningRightBTreeASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();
    auto result_values = join.result.collect_values();
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
    Prices prices = load_prices("../test/data/btc_usd_data.csv", ',', true);
    OrderBook order_book = load_order_book("../test/data/btc_orderbook_medium.csv", ',', true);
    PartitioningRightBTreeASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.size, 10000);
    ASSERT_EQ(join.result.value_sum, 70580346356);
}

TEST(asof_join_left_partitioning_btree, TestSmallDuckDBExample) {
    Prices prices = load_prices("../test/data/prices_small.csv", ',', true);
    OrderBook order_book = load_order_book("../test/data/orderbook_small.csv", ',', true);
    PartitioningLeftBTreeASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.size, 4);
    ASSERT_EQ(join.result.value_sum, 9581);
}

TEST(asof_join_left_partitioning_btree, TestSmallBTCExample) {
    Prices prices = load_prices("../test/data/btc_usd_data.csv", ',', true);
    OrderBook order_book = load_order_book("../test/data/btc_orderbook_small.csv", ',', true);
    PartitioningLeftBTreeASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();
    auto result_values = join.result.collect_values();
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
    Prices prices = load_prices("../test/data/btc_usd_data.csv", ',', true);
    OrderBook order_book = load_order_book("../test/data/btc_orderbook_medium.csv", ',', true);
    PartitioningLeftBTreeASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.size, 10000);
    ASSERT_EQ(join.result.value_sum, 70580346356);
}

TEST(asof_join_both_partitioning_sort_left, TestSmallDuckDBExample) {
    Prices prices = load_prices("../test/data/prices_small.csv", ',', true);
    OrderBook order_book = load_order_book("../test/data/orderbook_small.csv", ',', true);
    PartitioningBothSortLeftASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.size, 4);
    ASSERT_EQ(join.result.value_sum, 9581);
}

TEST(asof_join_both_partitioning_sort_left, TestSmallBTCExample) {
    Prices prices = load_prices("../test/data/btc_usd_data.csv", ',', true);
    OrderBook order_book = load_order_book("../test/data/btc_orderbook_small.csv", ',', true);
    PartitioningBothSortLeftASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();
    auto result_values = join.result.collect_values();
    std::sort(result_values.begin(), result_values.end());

    std::vector<uint64_t> correct_values = {
        5244, 9632, 247800, 523980, 4648032, 11277600, 11787330, 13727200, 33081768, 35807400
    };
    ASSERT_EQ(result_values.size(), correct_values.size());
    for (size_t i = 0; i < result_values.size(); ++i) {
        ASSERT_EQ(result_values[i], correct_values[i]) << "Failed at index " << i;
    }
}

TEST(asof_join_both_partitioning_sort_left, TestMediumBTCExample) {
    Prices prices = load_prices("../test/data/btc_usd_data.csv", ',', true);
    OrderBook order_book = load_order_book("../test/data/btc_orderbook_medium.csv", ',', true);
    PartitioningBothSortLeftASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.size, 10000);
    ASSERT_EQ(join.result.value_sum, 70580346356);
}

TEST(asof_join_both_partitioning_sort_left, TestSmallUniformExample) {
    Prices prices = load_prices("../test/data/uniform_small_prices.csv", ',', true);
    OrderBook order_book = load_order_book("../test/data/uniform_small_positions.csv", ',', true);
    PartitioningBothSortLeftASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.size, 10000);
    ASSERT_EQ(join.result.value_sum, 24168000);
}

TEST(asof_join_both_partitioning_sort_left, TestSmallZipfExample) {
    Prices prices = load_prices("../test/data/zipf_small_prices.csv", ',', true);
    OrderBook order_book = load_order_book("../test/data/zipf_small_positions.csv", ',', true);
    PartitioningBothSortLeftASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.size, 1024);
    ASSERT_EQ(join.result.value_sum, 2532396);
}

TEST(asof_join_partitioning_left_filter_min, TestSmallDuckDBExample) {
    Prices prices = load_prices("../test/data/prices_small.csv", ',', true);
    OrderBook order_book = load_order_book("../data/orderbook_small.csv", ',', true);
    PartitioningLeftFilterMinASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.size, 4);
    ASSERT_EQ(join.result.value_sum, 9581);
}

TEST(asof_join_partitioning_left_filter_min, TestSmallBTCExample) {
    Prices prices = load_prices("../test/data/btc_usd_data.csv", ',', true);
    OrderBook order_book = load_order_book("../data/btc_orderbook_small.csv", ',', true);
    PartitioningLeftFilterMinASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();
    auto result_values = join.result.collect_values();
    std::sort(result_values.begin(), result_values.end());

    std::vector<uint64_t> correct_values = {
        5244, 9632, 247800, 523980, 4648032, 11277600, 11787330, 13727200, 33081768, 35807400
    };
    ASSERT_EQ(result_values.size(), correct_values.size());
    for (size_t i = 0; i < result_values.size(); ++i) {
        ASSERT_EQ(result_values[i], correct_values[i]) << "Failed at index " << i;
    }
}

TEST(asof_join_partitioning_left_filter_min, TestMediumBTCExample) {
    Prices prices = load_prices("../test/data/btc_usd_data.csv", ',', true);
    OrderBook order_book = load_order_book("../data/btc_orderbook_medium.csv", ',', true);
    PartitioningLeftFilterMinASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.size, 10000);
    ASSERT_EQ(join.result.value_sum, 70580346356);
}

TEST(asof_join_partitioning_left_filter_min, TestSmallUniformExample) {
    Prices prices = load_prices("../test/data/uniform_small_prices.csv", ',', true);
    OrderBook order_book = load_order_book("../data/uniform_small_positions.csv", ',', true);
    PartitioningLeftFilterMinASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.size, 10000);
    ASSERT_EQ(join.result.value_sum, 24168000);
}

TEST(asof_join_partitioning_left_filter_min, TestSmallZipfExample) {
    Prices prices = load_prices("../test/data/zipf_small_prices.csv", ',', true);
    OrderBook order_book = load_order_book("../test/data/zipf_small_positions.csv", ',', true);
    PartitioningLeftFilterMinASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.size, 1024);
    ASSERT_EQ(join.result.value_sum, 2532396);
}

TEST(asof_join_partitioning_left_filter_min, TestSmallFilterMinExample) {
    Prices prices = load_prices("../test/data/small_filter_min_prices.csv", ',');
    OrderBook order_book = load_order_book("../test/data/small_filter_min_positions.csv", ',');
    PartitioningLeftFilterMinASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);
    //PartitioningLeftASOFJoin join(prices, order_book, LESS_EQUAL_THAN, INNER);

    join.join();

    ASSERT_EQ(join.result.size, 3);
    ASSERT_EQ(join.result.value_sum, 705);
}


