#include "asof_join.hpp"
#include "timer.hpp"
#include <unordered_map>

uint64_t BaselineASOFJoin::join() {
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
            result.insert(
                prices.timestamps[match_idx],
                prices.stock_ids[match_idx],
                prices.prices[match_idx],
                order_book.timestamps[i],
                order_book.stock_ids[i],
                order_book.amounts[i]
            );
        }
    }

    result.finalize();
    return -1;
}