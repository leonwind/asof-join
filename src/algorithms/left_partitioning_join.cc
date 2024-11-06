#include "asof_join.hpp"
#include "timer.hpp"
#include <unordered_map>
#include <algorithm>
#include <mutex>
#include "tbb/parallel_for.h"
#include "tbb/parallel_for_each.h"
#include "tbb/parallel_sort.h"
#include "tbb/concurrent_hash_map.h"


std::pair<bool, size_t> binary_search_closest_match_less_than(
        std::vector<std::pair<uint64_t, uint64_t>>& data,
        uint64_t target) {
    size_t left = 0;
    size_t right = data.size();

    while (left < right) {
        size_t mid = left + (right - left) / 2;

        if (data[mid].first <= target) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    return {left > 0 && data[left - 1].first <= target, left - 1};
}

ResultRelation PartitioningLeftASOFJoin::join() {
    ResultRelation result(prices, order_book);

    Timer timer;
    timer.start();

    std::unordered_map<std::string_view, std::vector<std::pair<uint64_t, uint64_t>>>
    //tbb::concurrent_hash_map<std::string_view, std::vector<std::pair<uint64_t, uint64_t>>>
        prices_lookup;

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

    std::cout << "Partitioning in " << timer.lap() << std::endl;

    tbb::parallel_for_each(prices_lookup.begin(), prices_lookup.end(),
                           [&](auto& iter) {
        tbb::parallel_sort(iter.second.begin(), iter.second.end());
        //std::sort(iter.second.begin(), iter.second.end());
    });

    std::cout << "Sorting in " << timer.lap() << std::endl;

    std::mutex result_lock;
    tbb::parallel_for(tbb::blocked_range<size_t>(0, order_book.size, 10000),
            [&](tbb::blocked_range<size_t>& range) {
        for (size_t i = range.begin(); i < range.end(); ++i) {
            auto& stock_id = order_book.stock_ids[i];
            auto timestamp = order_book.timestamps[i];

            auto& values = prices_lookup[stock_id];
            if (values.empty()) {
                continue;
            }

            auto [found_join_partner, match_idx] =
                binary_search_closest_match_less_than(
                    /* data= */ values,
                    /* target= */ timestamp);

            if (found_join_partner) {
                std::scoped_lock lock{result_lock};
                result.insert(
                    /* price_timestamp= */values[match_idx].first,
                    /* price_stock_id= */ stock_id,
                    /* price= */ values[match_idx].second,
                    /* order_book_timestamp= */ timestamp,
                    /* order_book_stock_id= */ order_book.stock_ids[i],
                    /* amount= */ order_book.amounts[i]);
            }
        }
    });

    std::cout << "Binary Search in " << timer.lap() << std::endl;

    result.finalize();
    return result;
}