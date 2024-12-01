#include "asof_join.hpp"
#include "timer.hpp"
#include "log.hpp"
#include "parallel_multi_map.hpp"
#include <fmt/format.h>
#include <algorithm>
#include <unordered_map>
#include <mutex>
#include "tbb/parallel_sort.h"
#include "tbb/parallel_for.h"
#include "tbb/parallel_for_each.h"


// Morsel size is 16384
#define MORSEL_SIZE (2<<14)

using JoinAlg = PartitioningBothSortRightASOFJoin;

inline JoinAlg::RightEntry* JoinAlg::binary_search_closest_match_greater_than(
        std::vector<RightEntry> &data, uint64_t target) {
    auto iter = std::upper_bound(data.begin(), data.end(), target,
        [](uint64_t a, const RightEntry& b) {
            return a <= b.timestamp;
    });

    if (iter == data.end()) {
        return nullptr;
    }

    return &(*iter);
}

namespace {
    size_t subset_binary_search_closest_match(
            std::vector<JoinAlg::RightEntry>& data,
            uint64_t target,
            size_t start_offset = 0,
            size_t end_offset = 0) {
        auto start_iter = data.begin() + start_offset;
        auto end_iter = data.end() - end_offset;
        //std::cout << (end_iter - data.begin()) - (start_iter - data.begin()) << ", " << data.size() << std::endl;
        //std::cout << start_iter - data.begin() << ", " << end_iter - data.begin() << std::endl;
        auto iter = std::upper_bound(
            start_iter,
            end_iter,
            target,
            [](uint64_t a, const JoinAlg::RightEntry &b) {
                return a <= b.timestamp;
        });

        return iter != end_iter
            ? iter - data.begin()
            : UINT64_MAX;
    }
} // namespace

uint64_t JoinAlg::join() {
    PerfEvent e;
    Timer<milliseconds> timer;
    timer.start();

    //e.startCounters();
    MultiMap<RightEntry> order_book_lookup(order_book.stock_ids, order_book.timestamps);
    //log(fmt::format("Right Partitioning in {}{}", timer.lap(), timer.unit()));

    MultiMap<LeftEntry> prices_lookup(prices.stock_ids, prices.timestamps);
    //log(fmt::format("Left Partitioning in {}{}", timer.lap(), timer.unit()));
    //e.stopCounters();
    //log("Right Partitioning Perf");
    //e.printReport(std::cout, order_book.size);

    //e.startCounters();
    tbb::parallel_for_each(order_book_lookup.begin(), order_book_lookup.end(),
            [&](auto& iter) {
        tbb::parallel_sort(iter.second.begin(), iter.second.end());
    });

    //e.stopCounters();
    //log("\n\nSorting Left Perf: ");
    //e.printReport(std::cout, prices.size);
    //log(fmt::format("Sorting Left in {}{}", timer.lap(), timer.unit()));

    //e.startCounters();
    tbb::parallel_for_each(prices_lookup.begin(), prices_lookup.end(),
        [&](auto& iter) {
            const auto& stock_id = iter.first;
            std::vector<LeftEntry>& prices_partition_bin = iter.second;
            if (!order_book_lookup.contains(stock_id)) {
                return;
            }

            auto& order_partition_bin = order_book_lookup[stock_id];

            //uint64_t last_target = UINT64_MAX;
            //size_t last_pos = UINT64_MAX;
            size_t start_offset = 0;
            size_t end_offset = 0;
            tbb::parallel_for(tbb::blocked_range<size_t>(0, prices_partition_bin.size()),
                    [&](auto& range) {
                for (size_t i = range.begin(); i < range.end(); ++i) {
                    auto& timestamp = prices_partition_bin[i].timestamp;

                    //if (last_target != UINT64_MAX && last_pos != 0) [[likely]] {
                    //    start_offset = last_pos * (timestamp >= last_target);
                    //    end_offset = (order_partition_bin.size() - (last_pos + 1)) * (timestamp <= last_target);
                    //}

                    auto entry_pos = subset_binary_search_closest_match(
                        order_partition_bin,
                        timestamp,
                        start_offset,
                        end_offset);

                    //last_target = timestamp;
                    //last_pos = entry_pos == UINT64_MAX ? 0 : entry_pos;

                    if (entry_pos == UINT64_MAX) {
                        continue;
                    }

                    auto& match = order_partition_bin[entry_pos];
                    uint64_t diff = match.timestamp - timestamp;
                    /// (Spin) Lock while comparing and exchanging the diffs
                    /// CAS is probably too expensive on two 64 Bit fields.
                    match.lock.lock();
                    if (diff < match.diff) {
                        match.diff = diff;
                        match.price_idx = prices_partition_bin[i].idx;
                    }
                    match.lock.unlock();

                    match.matched = true;
                }
            });
    });
    //e.stopCounters();
    //std::cout << "\n\nBinary Search Perf: " << std::endl;
    //e.printReport(std::cout, prices.size);
    //log(fmt::format("Binary Search in {}{}", timer.lap(), timer.unit()));

    //e.startCounters();
    std::mutex result_lock;
    tbb::parallel_for_each(order_book_lookup.begin(), order_book_lookup.end(),
            [&](auto& iter) {
        std::vector<RightEntry>& partition_bin = iter.second;
        const size_t num_thread_chunks = (partition_bin.size() + MORSEL_SIZE - 1) / MORSEL_SIZE;
        std::vector<RightEntry*> last_match_per_range(num_thread_chunks);

        tbb::parallel_for(tbb::blocked_range<size_t>(0, partition_bin.size(), MORSEL_SIZE),
                [&](tbb::blocked_range<size_t>& range) {
            RightEntry* last_match = nullptr;
            for (size_t i = range.end(), j = i - 1; i != range.begin(); --i) {
                if (partition_bin[j].matched) {
                    last_match = &partition_bin[j];
                    break;
                }
            }

            last_match_per_range[range.begin() / MORSEL_SIZE] = last_match;
        });

        tbb::parallel_for(tbb::blocked_range<size_t>(0, partition_bin.size(), MORSEL_SIZE),
                [&](tbb::blocked_range<size_t>& range) {
            RightEntry* last_match = nullptr;
            size_t morsel_pos = range.begin() / MORSEL_SIZE;
            for (size_t i = morsel_pos; i != 0; --i) {
                if (last_match_per_range[i - 1] != nullptr) {
                    last_match = last_match_per_range[i - 1];
                    break;
                }
            }

            for (size_t i = range.begin(); i != range.end(); ++i) {
                auto& entry = partition_bin[i];
                if (entry.matched) {
                    last_match = &entry;
                }

                if (last_match && last_match->matched) {
                    std::scoped_lock lock{result_lock};
                    result.insert(
                        /* price_timestamp= */ prices.timestamps[last_match->price_idx],
                        /* price_stock_id= */ prices.stock_ids[last_match->price_idx],
                        /* price= */ prices.prices[last_match->price_idx],
                        /* order_book_timestamp= */ order_book.timestamps[entry.order_idx],
                        /* order_book_stock_id= */ order_book.stock_ids[entry.order_idx],
                        /* amount= */ order_book.amounts[entry.order_idx]);
                }
            }
        });
    });
    //e.stopCounters();
    //std::cout << "\n\nFinding Match Perf: " << std::endl;
    //e.printReport(std::cout, prices.size);
    //log(fmt::format("Finding match in {}{}", timer.lap(), timer.unit()));

    result.finalize();
    return timer.stop();
}
