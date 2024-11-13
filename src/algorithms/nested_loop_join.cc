#include "asof_join.hpp"
#include "timer.hpp"
#include <unordered_map>

void BaselineASOFJoin::join() {
    for (size_t i = 0; i < order_book.size; ++i) {
        bool found_join_partner = false;
        size_t match_idx = 0;
        size_t min_distance = INT64_MAX;

        for (size_t j = 0; j < prices.size; ++j) {
            if (order_book.stock_ids[i] != prices.stock_ids[j]) {
                continue;
            }

            size_t curr_distance = order_book.timestamps[i] - prices.timestamps[j];
            if (order_book.timestamps[i] >= prices.timestamps[j]
                && curr_distance < min_distance) {
                match_idx = j;
                min_distance = curr_distance;
                found_join_partner = true;
            }
        }

        if (found_join_partner) {
            result.prices_timestamps.push_back(prices.timestamps[match_idx]);
            result.prices_stock_ids.push_back(prices.stock_ids[match_idx]);
            result.prices.push_back(prices.prices[match_idx]);
            result.order_book_timestamps.push_back(order_book.timestamps[i]);
            result.order_book_stock_ids.push_back(order_book.stock_ids[i]);
            result.amounts.push_back(order_book.amounts[i]);
            result.values.push_back(prices.prices[match_idx] * order_book.amounts[i]);
        }
    }

    result.finalize();
}