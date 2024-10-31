#include "asof_join.hpp"
#include "timer.hpp"
#include <unordered_map>
#include <algorithm>
#include <cassert>
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

    Timer timer;
    timer.start();

    std::vector<size_t> prices_indices(prices.size);
    for (size_t i = 0; i < prices.size; ++i) { prices_indices[i] = i; }
    std::sort(prices_indices.begin(), prices_indices.end(),
        [&](size_t i, size_t j) {
        return prices.stock_ids[i] != prices.stock_ids[j]
            ? prices.stock_ids[i] < prices.stock_ids[j]
            : prices.timestamps[i] < prices.timestamps[j];
    });

    std::cout << "Sorted prices in " << timer.lap() << std::endl;

    std::vector<size_t> order_book_indices(order_book.size);
    for (size_t i = 0; i < order_book.size; ++i) { order_book_indices[i] = i; }
    std::sort(order_book_indices.begin(), order_book_indices.end(),
        [&](size_t i, size_t j) {
        return order_book.stock_ids[i] != order_book.stock_ids[j]
            ? order_book.stock_ids[i] < order_book.stock_ids[j]
            : order_book.timestamps[i] < order_book.timestamps[j];
    });

    std::cout << "Sorted order book in " << timer.lap() << std::endl;

    size_t i = 0, j = 0;
    while (i < prices.size && j < order_book.size) {
        size_t price_idx = prices_indices[i];
        size_t order_book_idx = order_book_indices[j];

        if (prices.stock_ids[price_idx] < order_book.stock_ids[order_book_idx]) {
            ++i;
            continue;
        } else if (prices.stock_ids[price_idx] > order_book.stock_ids[order_book_idx]) {
            ++j;
            continue;
        }

        assert(prices.stock_ids[price_idx] == order_book.stock_ids[order_book_idx]);

        size_t last_match = i;
        bool found_match = false;
        while (i < prices.size &&
            prices.stock_ids[price_idx] == order_book.stock_ids[order_book_idx] &&
            prices.timestamps[price_idx] <= order_book.timestamps[order_book_idx]) {

            last_match = i;
            ++i;
            found_match = true;
            price_idx = prices_indices[i];
        }

        if (found_match) {
            price_idx = prices_indices[last_match];

            result.prices_timestamps.push_back(prices.timestamps[price_idx]);
            result.prices_stock_ids.push_back(prices.stock_ids[price_idx]);
            result.prices.push_back(prices.prices[price_idx]);

            result.order_book_timestamps.push_back(order_book.timestamps[order_book_idx]);
            result.order_book_stock_ids.push_back(order_book.stock_ids[order_book_idx]);
            result.amounts.push_back(order_book.amounts[order_book_idx]);

            result.values.push_back(
                prices.prices[price_idx] * order_book.amounts[order_book_idx]);
        }

        ++j;
        i = last_match;
    }

    std::cout << "Sorted merge join in " << timer.lap() << std::endl;

    result.finalize();
    return result;
}

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
        std::vector<Entry>& data,
        uint64_t target, size_t left, size_t right) {
    size_t result_index = right;
    bool found = false;

    while (left < right) {
        size_t mid = left + (right - left) / 2;

        if (data[mid].timestamp >= target) {
            result_index = mid;
            found = true;
            right = mid;
        } else {
            left = mid + 1;
        }
    }

    if (left == right && data[left].timestamp >= target) {
        return {true, left};
    }
    return {found, result_index};
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
