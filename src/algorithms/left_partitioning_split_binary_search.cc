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

using JoinAlg = PartitioningLeftSplitBinarySearchASOFJoin;

namespace {
    size_t subset_binary_search_closest_match(
            std::vector<JoinAlg::Entry>& data,
            uint64_t target,
            size_t start_offset = 0,
            size_t end_offset = 0) {
        auto start_iter = data.begin() + start_offset;
        auto end_iter = data.end() - end_offset;
        //std::cout << (end_iter - data.begin()) - (start_iter - data.begin()) << ", " << data.size() << std::endl;
        auto iter = std::lower_bound(
            start_iter,
            end_iter,
            target,
            [](const JoinAlg::Entry& a, uint64_t b) {
                return a.timestamp <= b;
        });

        return iter != start_iter
            ? (--iter) - data.begin()
            : UINT64_MAX;
    }
} // namespace

void JoinAlg::join() {
    Timer<milliseconds> timer;
    timer.start();
    PerfEvent e;

    e.startCounters();
    MultiMap<Entry> prices_lookup(prices.stock_ids, prices.timestamps);
    e.stopCounters();
    log("Partitioning Perf");
    e.printReport(std::cout, prices.size);
    log(fmt::format("Partitioning in {}{}", timer.lap(), timer.unit()));

    e.startCounters();
    tbb::parallel_for_each(prices_lookup.begin(), prices_lookup.end(),
            [&](auto& iter) {
        tbb::parallel_sort(iter.second.begin(), iter.second.end());
    });
    log(fmt::format("Sorting in {}{}", timer.lap(), timer.unit()));
    e.stopCounters();
    std::cout << "\n\nSorting Perf: " << std::endl;
    e.printReport(std::cout, prices.size);

    e.startCounters();
    std::mutex result_lock;
    tbb::parallel_for(tbb::blocked_range<size_t>(0, order_book.size, MORSEL_SIZE),
            [&](tbb::blocked_range<size_t>& range) {

        uint64_t last_target = UINT64_MAX;
        size_t last_pos = UINT64_MAX;
        for (size_t i = range.begin(); i < range.end(); ++i) {
            auto& stock_id = order_book.stock_ids[i];
            if (!prices_lookup.contains(stock_id)) {
               continue;
            }

            size_t start_offset = 0;
            size_t end_offset = 0;

            auto& partition_bin = prices_lookup[stock_id];
            auto timestamp = order_book.timestamps[i];
            if (last_target != UINT64_MAX) [[likely]] {
                start_offset = last_pos * (timestamp >= last_target);
                end_offset = (partition_bin.size() - last_pos) * (timestamp <= last_target);
            }

            auto entry_pos = subset_binary_search_closest_match(
                partition_bin,
                timestamp,
                start_offset,
                end_offset);

            last_target = timestamp;
            last_pos = entry_pos == UINT64_MAX ? 0 : entry_pos;

            if (entry_pos == UINT64_MAX) {
                continue;
            }

            auto& match = partition_bin[entry_pos];
            std::scoped_lock lock{result_lock};
            result.insert(
                /* price_timestamp= */match.timestamp,
                /* price_stock_id= */ stock_id,
                /* price= */ prices.prices[match.idx],
                /* order_book_timestamp= */ timestamp,
                /* order_book_stock_id= */ order_book.stock_ids[i],
                /* amount= */ order_book.amounts[i]);
        }
    });
    e.stopCounters();
    std::cout << "\n\nBinary Search Perf: " << std::endl;
    e.printReport(std::cout, prices.size);
    log(fmt::format("Binary Search in {}{}", timer.lap(), timer.unit()));

    result.finalize();
}