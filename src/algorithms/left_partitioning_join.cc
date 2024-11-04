#include "asof_join.hpp"
#include "timer.hpp"
#include <unordered_map>
#include <algorithm>


std::pair<bool, size_t> binary_search_closest_match_less_than(
        std::vector<std::pair<uint64_t, uint64_t>>& data,
        uint64_t target, size_t left, size_t right) {
    size_t result_index = right;
    bool found = false;

    while (left < right) {
        size_t mid = left + (right - left) / 2;

        if (data[mid].first <= target) {
            result_index = mid;
            found = true;
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    if (left == right && data[left].first <= target) {
        return {true, left};
    }
    return {found, result_index};
}

ResultRelation PartitioningLeftASOFJoin::join() {
    ResultRelation result(prices, order_book);

    Timer timer;
    timer.start();

    std::unordered_map<std::string_view, std::vector<std::pair<uint64_t, uint64_t>>>
        prices_lookup;
    prices_lookup.reserve(prices.size);

    for (size_t i = 0; i < prices.size; ++i) {
        if (prices_lookup.contains(prices.stock_ids[i])) {
            prices_lookup[prices.stock_ids[i]].emplace_back(
                prices.timestamps[i],
                prices.prices[i]
            );
        } else {
            prices_lookup[prices.stock_ids[i]] = {{
                prices.timestamps[i],
                prices.prices[i]
            }};
        }
    }

    std::cout << "Partioning in " << timer.lap() << std::endl;

    for (auto& iter : prices_lookup) {
        std::sort(iter.second.begin(), iter.second.end());
    }

    std::cout << "Sorting in " << timer.lap() << std::endl;

    for (size_t i = 0; i < order_book.size; ++i) {
        if (!prices_lookup.contains(order_book.stock_ids[i])) {
            continue;
        }

        auto& values = prices_lookup[order_book.stock_ids[i]];
        const auto& [found_join_partner, match_idx] =
            binary_search_closest_match_less_than(
                values, order_book.timestamps[i], 0, values.size() - 1);

        if (found_join_partner) {
            result.prices_timestamps.push_back(values[match_idx].first);
            result.prices_stock_ids.push_back(order_book.stock_ids[i]);
            result.prices.push_back(values[match_idx].second);
            result.order_book_timestamps.push_back(order_book.timestamps[i]);
            result.order_book_stock_ids.push_back(order_book.stock_ids[i]);
            result.amounts.push_back(order_book.amounts[i]);
            result.values.push_back(values[match_idx].second * order_book.amounts[i]);
        }
    }

    std::cout << "Binary Search in " << timer.lap() << std::endl;

    result.finalize();
    return result;
}