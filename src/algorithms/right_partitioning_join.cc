#include "asof_join.hpp"
#include "timer.hpp"
#include <unordered_map>
#include <algorithm>
#include <cassert>


struct Entry {
    uint64_t timestamp;
    size_t order_idx;
    size_t price_idx;
    uint64_t diff;
    bool matched;

    Entry(uint64_t timestamp, size_t order_idx):
        timestamp(timestamp), order_idx(order_idx), price_idx(0),
        diff(UINT64_MAX), matched(false) {}

    std::strong_ordering operator <=>(const Entry& other) const {
        return timestamp <=> other.timestamp;
    }
};

std::pair<bool, size_t> binary_search_closest_match_greater_than(
        const std::vector<Entry>& data,
        uint64_t target, size_t left, size_t right) {

    while (left < right) {
        size_t mid = left + (right - left) / 2;

        if (data[mid].timestamp < target) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    return {left < data.size() && data[left].timestamp >= target, left};
}

ResultRelation PartitioningRightASOFJoin::join() {
    ResultRelation result(prices, order_book);

    Timer timer;
    timer.start();

    std::unordered_map<std::string_view, std::vector<Entry>>
        order_book_lookup;
    order_book_lookup.reserve(order_book.size);

    for (size_t i = 0; i < order_book.size; ++i) {
        if (order_book_lookup.contains(order_book.stock_ids[i])) {
            order_book_lookup[order_book.stock_ids[i]].emplace_back(
                order_book.timestamps[i], i
            );
        } else {
            order_book_lookup[order_book.stock_ids[i]] = std::vector<Entry>{
                {order_book.timestamps[i], i}
            };
        }
    }

    std::cout << "Partioning in " << timer.lap() << std::endl;

    for (auto& iter : order_book_lookup) {
        std::sort(iter.second.begin(), iter.second.end());
    }

    std::cout << "Sorting in " << timer.lap() << std::endl;

    for (size_t i = 0; i < prices.size; ++i) {
        if (!order_book_lookup.contains(prices.stock_ids[i])) {
            continue;
        }

        auto& values = order_book_lookup[prices.stock_ids[i]];
        const auto& [found_join_partner, match_idx] =
            binary_search_closest_match_greater_than(
                values, prices.timestamps[i], 0, values.size() - 1);

        if (!found_join_partner) {
            continue;
        }

        size_t order_book_idx = values[match_idx].order_idx;
        assert(order_book.timestamps[order_book_idx] >= prices.timestamps[i]);

        uint64_t diff = order_book.timestamps[order_book_idx] - prices.timestamps[i];
        if (diff < values[match_idx].diff) {
           values[match_idx].diff = diff;
           values[match_idx].price_idx = i;
        }
        values[match_idx].matched = true;
    }

     std::cout << "Binary Search in " << timer.lap() << std::endl;

    for (auto& iter : order_book_lookup) {
        auto& values = iter.second;

        Entry* last_match = nullptr;
        for (auto& entry : values) {
            if (entry.matched) {
                last_match = &entry;
            }

            if (last_match && last_match->matched) {
                result.prices_timestamps.push_back(prices.timestamps[entry.price_idx]);
                result.prices_stock_ids.push_back(prices.stock_ids[entry.price_idx]);
                result.prices.push_back(prices.prices[entry.price_idx]);

                result.order_book_timestamps.push_back(order_book.timestamps[entry.order_idx]);
                result.order_book_stock_ids.push_back(order_book.stock_ids[entry.order_idx]);
                result.amounts.push_back(order_book.amounts[entry.order_idx]);
                result.values.push_back(
                    prices.prices[last_match->price_idx] * order_book.amounts[entry.order_idx]);
            }
        }
    }

    std::cout << "Finding match in " << timer.lap() << std::endl;

    result.finalize();
    return result;
}
