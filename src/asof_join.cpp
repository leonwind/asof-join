#include "asof_join.h"
#include "timer.h"
#include <unordered_map>
#include <algorithm>

ResultRelation BaselineASOFJoin::join() {
    ResultRelation result(prices, order_book);

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
    return result;
}

ResultRelation SortingASOFJoin::join() {
    ResultRelation result(prices, order_book);
    result.finalize();
    return result;
}

std::pair<bool, size_t> binary_search_closest_match(
        std::vector<std::pair<uint64_t, uint64_t>>& data,
        uint64_t target, size_t left, size_t right) {
    if (left == right) {
        return {data[left].first <= target, left};
    }

    size_t mid = left + (right - left) / 2;
    if (target < data[mid].first) {
        return binary_search_closest_match(data, target, left, mid);
    }

    auto result=
        binary_search_closest_match(data, target, mid + 1, right);
    return !result.first ? std::make_pair(true, mid) : result;
}

ResultRelation PartitioningASOFJoin::join() {
    ResultRelation result(prices, order_book);

    Timer hash_timer = Timer::start();
    std::unordered_map<std::string_view, std::vector<std::pair<uint64_t, uint64_t>>>
        prices_lookup;
    prices_lookup.reserve(prices.size);

    for (size_t i = 0; i < prices.size; ++i) {
        if (prices_lookup.contains(prices.stock_ids[i])) {
            prices_lookup[prices.stock_ids[i]].push_back({
                prices.timestamps[i],
                prices.prices[i]
            });
        } else {
            prices_lookup[prices.stock_ids[i]] = {{
                prices.timestamps[i],
                prices.prices[i]
            }};
        }
    }

    for (auto& iter : prices_lookup) {
        std::sort(iter.second.begin(), iter.second.end());
    }

    auto duration = hash_timer.end();
    std::cout << "HASHING AND SORTING IN " << duration << std::endl;

    for (size_t i = 0; i < order_book.size; ++i) {
        if (!prices_lookup.contains(order_book.stock_ids[i])) {
            continue;
        }

        //bool found_join_partner = false;
        //size_t match_idx = 0;
        auto& values = prices_lookup[order_book.stock_ids[i]];

        const auto& [found_join_partner, match_idx] =
            binary_search_closest_match(values, order_book.timestamps[i], 0, values.size());

        /*
        for (size_t j = 0; j < values.size(); ++j) {
            auto timestamp = values[j].first;
            if (timestamp > order_book.timestamps[i]) {
                break;
            }
            match_idx = j;
            found_join_partner = true;
        }*/

        if (found_join_partner) {
            result.prices_timestamps.push_back(values[match_idx].first);
            result.prices_stock_ids.push_back(order_book.stock_ids[i]);
            result.prices.push_back(values[match_idx].second);
            result.order_book_timestamps.push_back(order_book.timestamps[i]);
            result.order_book_stock_ids.push_back(order_book.stock_ids[i]);
            result.amounts.push_back(order_book.amounts[i]);
            result.values.push_back(prices.prices[match_idx] * order_book.amounts[i]);
        }
    }

    result.finalize();
    return result;
}

