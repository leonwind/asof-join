#include "asof_join.hpp"
#include "timer.hpp"
#include "log.hpp"
#include "parallel_multi_map.hpp"
#include <fmt/core.h>
#include <algorithm>
#include <unordered_map>
#include <mutex>
#include "tbb/parallel_for.h"
#include "tbb/parallel_for_each.h"
#include "tbb/parallel_sort.h"


// Morsel size is 16384
#define MORSEL_SIZE (2<<14)

std::optional<size_t> PartitioningLeftASOFJoin::binary_search_closest_match_less_than(
        std::vector<Entry>& data, uint64_t target) {
    auto iter = std::lower_bound(data.begin(), data.end(), target,
        [](const Entry& a, uint64_t b) {
            return a.timestamp <= b;
    });

    if (iter == data.begin()) {
        return {};
    }

    return {(iter - 1) - data.begin()};
}

void PartitioningLeftASOFJoin::join() {
    Timer<milliseconds> timer;
    timer.start();

    MultiMap<Entry> prices_lookup(prices.stock_ids, prices.timestamps);
    log(fmt::format("Partitioning in {}{}", timer.lap(), timer.unit()));

    tbb::parallel_for_each(prices_lookup.begin(), prices_lookup.end(),
            [&](auto& iter) {
        tbb::parallel_sort(iter.second.begin(), iter.second.end());
    });
    log(fmt::format("Sorting in {}{}", timer.lap(), timer.unit()));

    std::mutex result_lock;
    tbb::parallel_for(tbb::blocked_range<size_t>(0, order_book.size, MORSEL_SIZE),
            [&](tbb::blocked_range<size_t>& range) {
        for (size_t i = range.begin(); i < range.end(); ++i) {
            auto& stock_id = order_book.stock_ids[i];
            auto& timestamp = order_book.timestamps[i];

            if (!prices_lookup.contains(stock_id)) {
               continue;
            }
            auto& partition_bin = prices_lookup[stock_id];

            auto match_idx_opt = binary_search_closest_match_less_than(
                /* data= */ partition_bin,
                /* target= */ timestamp);

            if (match_idx_opt.has_value()) {
                size_t match_idx = match_idx_opt.value();

                std::scoped_lock lock{result_lock};
                result.insert(
                    /* price_timestamp= */partition_bin[match_idx].timestamp,
                    /* price_stock_id= */ stock_id,
                    /* price= */ prices.prices[partition_bin[match_idx].idx],
                    /* order_book_timestamp= */ timestamp,
                    /* order_book_stock_id= */ order_book.stock_ids[i],
                    /* amount= */ order_book.amounts[i]);
            }
        }
    });
    log(fmt::format("Binary Search in {}{}", timer.lap(), timer.unit()));

    result.finalize();
}