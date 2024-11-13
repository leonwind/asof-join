#include "asof_join.hpp"
#include "timer.hpp"
#include "log.hpp"
#include <fmt/core.h>
#include <algorithm>
#include <unordered_map>
#include <mutex>
#include "tbb/parallel_for.h"
#include "tbb/parallel_for_each.h"
#include "tbb/parallel_sort.h"


// Morsel size is 16384
#define MORSEL_SIZE (2<<14)

namespace {
std::optional<size_t> binary_search_closest_match_less_than(
        std::vector<std::pair<uint64_t, uint64_t>> &data,
        uint64_t target) {
    auto iter = std::lower_bound(data.begin(), data.end(), target,
                                 [](const std::pair<uint64_t, uint64_t> &a, uint64_t b) {
                                     return a.first <= b;
                                 });

    if (iter == data.begin()) {
        return {};
    }

    return {(iter - 1) - data.begin()};
}
} // namespace

void PartitioningLeftASOFJoin::join() {
    Timer timer;
    timer.start();

    std::unordered_map<std::string_view, std::vector<std::pair<uint64_t, uint64_t>>>
        prices_lookup;
    for (size_t i = 0; i < prices.size; ++i) {
        prices_lookup[prices.stock_ids[i]].emplace_back(
            prices.timestamps[i],
            prices.prices[i]);
    }

    log(fmt::format("Partitioning in {}", timer.lap()));

    tbb::parallel_for_each(prices_lookup.begin(), prices_lookup.end(),
            [&](auto& iter) {
        tbb::parallel_sort(iter.second.begin(), iter.second.end());
    });

    log(fmt::format("Sorting in {}", timer.lap()));

    std::mutex result_lock;
    tbb::parallel_for(tbb::blocked_range<size_t>(0, order_book.size, MORSEL_SIZE),
            [&](tbb::blocked_range<size_t>& range) {
        for (size_t i = range.begin(); i < range.end(); ++i) {
            auto& stock_id = order_book.stock_ids[i];
            auto timestamp = order_book.timestamps[i];

            auto& values = prices_lookup[stock_id];
            if (values.empty()) {
                continue;
            }

            auto match_idx_opt = binary_search_closest_match_less_than(
                /* data= */ values,
                /* target= */ timestamp);

            if (match_idx_opt.has_value()) {
                size_t match_idx = match_idx_opt.value();

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

    log(fmt::format("Binary Search in {}", timer.lap()));

    result.finalize();
}