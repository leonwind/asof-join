#include "asof_join.hpp"
#include "timer.hpp"
#include "log.hpp"
#include "parallel_multi_map.hpp"
#include "searches.hpp"
#include <fmt/core.h>
#include <algorithm>
#include <unordered_map>
#include <mutex>
#include "tbb/parallel_for.h"
#include "tbb/parallel_for_each.h"
#include "tbb/parallel_sort.h"


// Morsel size is 16384
#define MORSEL_SIZE (2<<14)

void PartitioningRightFilterMinASOFJoin::join() {
    Timer<milliseconds> timer;
    timer.start();
    PerfEvent e;

    e.startCounters();
    MultiMapTB<RightEntry> prices_lookup(prices.stock_ids, prices.timestamps);
    e.stopCounters();
    log("Partitioning Perf");
    log(e.getReport(prices.size));
    log(fmt::format("Partitioning in {}{}", timer.lap(), timer.unit()));

    e.startCounters();
    tbb::parallel_for_each(prices_lookup.begin(), prices_lookup.end(),
            [&](auto& iter) {
        tbb::parallel_sort(iter.second.begin(), iter.second.end());
    });
    log(fmt::format("Sorting in {}{}", timer.lap(), timer.unit()));
    e.stopCounters();
    log("\n\nSorting Perf: ");
    log(e.getReport(prices.size));

    uint64_t global_min = UINT64_MAX;
    for (const auto& [_, partition] : prices_lookup) {
        if (!partition.empty()) {
            global_min = std::min(global_min, partition[0].timestamp);
        }
    }

    std::atomic<uint64_t> num_skipped_lookups = 0;

    e.startCounters();
    tbb::parallel_for(tbb::blocked_range<size_t>(0, order_book.size, MORSEL_SIZE),
            [&](tbb::blocked_range<size_t>& range) {
        for (size_t i = range.begin(); i < range.end(); ++i) {
            auto& stock_id = order_book.stock_ids[i];
            if (!prices_lookup.contains(stock_id)) {
               continue;
            }

            auto timestamp = order_book.timestamps[i];
            /// If the current timestamp is from before all prices timestamps, it cannot be matched.
            if (timestamp < global_min) {
                ++num_skipped_lookups;
                continue;
            }

            auto& partition_bin = prices_lookup[stock_id];
            auto* match = Search::Interpolation::less_equal_than(
                /* data= */ partition_bin,
                /* target= */ timestamp);

            if (match != nullptr) {
                result.insert(
                    /* price_timestamp= */match->timestamp,
                    /* price_stock_id= */ stock_id,
                    /* price= */ prices.prices[match->idx],
                    /* order_book_timestamp= */ timestamp,
                    /* order_book_stock_id= */ order_book.stock_ids[i],
                    /* amount= */ order_book.amounts[i]);
            }
        }
    });
    e.stopCounters();
    log("\n\nBinary Search Perf: ");
    log(e.getReport(prices.size));
    log(fmt::format("Binary Search in {}{}", timer.lap(), timer.unit()));
    log(fmt::format("Num lookups skipped: {}", num_skipped_lookups.load()));

    result.finalize();
}
