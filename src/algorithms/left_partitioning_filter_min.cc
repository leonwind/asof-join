#include "asof_join.hpp"
#include "timer.hpp"
#include "log.hpp"
#include "parallel_multi_map.hpp"
#include "searches.hpp"
#include <algorithm>
#include <fmt/format.h>
#include <unordered_map>
#include <mutex>
#include "tbb/parallel_sort.h"
#include "tbb/parallel_for.h"
#include "tbb/parallel_for_each.h"


// Morsel size is 16384
#define MORSEL_SIZE (2<<14)

void PartitioningLeftFilterMinASOFJoin::join() {
    PerfEvent e;
    Timer<milliseconds> timer;
    timer.start();

    //e.startCounters();
    MultiMapTB<LeftEntry> order_book_lookup(order_book.stock_ids, order_book.timestamps);
    //e.stopCounters();
    log("Partitioning Perf");
    //e.printReport(std::cout, order_book.size);
    log(fmt::format("Partitioning in {}{}", timer.lap(), timer.unit()));

    //e.startCounters();
    tbb::parallel_for_each(order_book_lookup.begin(), order_book_lookup.end(),
            [&](auto& iter) {
        tbb::parallel_sort(iter.second.begin(), iter.second.end());
    });
    //e.stopCounters();
    log("\n\nSorting Perf: ");
    //e.printReport(std::cout, prices.size);
    log(fmt::format("Sorting in {}{}", timer.lap(), timer.unit()));

    e.startCounters();
    std::atomic<uint64_t> global_min = 0;
    std::atomic<uint64_t> num_lookups_skipped = 0;
    tbb::parallel_for(tbb::blocked_range<size_t>(0, prices.size, MORSEL_SIZE),
            [&](tbb::blocked_range<size_t>& range) {
        for (size_t i = range.begin(); i < range.end(); ++i) {
            const auto& stock_id = prices.stock_ids[i];
            if (!order_book_lookup.contains(stock_id)) {
                continue;
            }

            auto timestamp = prices.timestamps[i];
            //std::cout << "Timestamp: " << timestamp << ", global min: " << global_min << std::endl;
            if (timestamp < global_min) {
                ++num_lookups_skipped;
                continue;
            }

            auto& partition_bin = order_book_lookup[stock_id];
            auto* match= Search::Interpolation::greater_equal_than(
                /* data= */ partition_bin,
                /* target= */ timestamp);

            if (match != nullptr) {
                uint64_t diff = match->timestamp - timestamp;
                match->atomic_compare_swap_diffs(diff, i);
            }
        }

        /// Update global min for filtering after each morsel chunk.
        /// Collect all the minimum matched timestamps by checking the first entry in each bucket.
        /// If all bucket heads have a match, the global min is the minimum of all bucket heads.
        uint64_t local_min = UINT64_MAX;
        bool all_partitions_have_match = true;
        for (const auto& [s_id, partition] : order_book_lookup) {
            bool partition_head_has_match = partition.empty() || partition[0].matched;
            all_partitions_have_match = all_partitions_have_match && partition_head_has_match;

            local_min = std::min(local_min, partition.empty() || !partition_head_has_match
                ? UINT64_MAX
                : prices.timestamps[partition[0].diff_price.load().price_idx]);
        }

        /// Use CAS to tighten the global minimum if the current minimum is greater.
        if (all_partitions_have_match) {
            uint64_t old_global_min = global_min.load(std::memory_order_relaxed);
            while (local_min > global_min) {
                if (global_min.compare_exchange_weak(
                        /* expected= */ old_global_min,
                        /* desired= */ local_min,
                        /* success= */ std::memory_order_acquire,
                        /* failure= */ std::memory_order_relaxed)) { break; }
            }
        }
    });
    e.stopCounters();
    log("\n\nBinary Search Perf:");
    log(e.getReport(prices.size));
    log(fmt::format("Binary Search in {}{}", timer.lap(), timer.unit()));
    log(fmt::format("Num lookups skipped: {}", num_lookups_skipped.load()));

    e.startCounters();
    tbb::parallel_for_each(order_book_lookup.begin(), order_book_lookup.end(),
            [&](auto& iter) {
        std::vector<LeftEntry>& partition_bin = iter.second;
        const size_t num_thread_chunks = (partition_bin.size() + MORSEL_SIZE - 1) / MORSEL_SIZE;
        std::vector<LeftEntry*> last_match_per_range(num_thread_chunks);

        /// We have to parallel iterate over the number of thread chunks since TBB is not forced to align
        /// each chunk to [[MORSEL_SIZE]] which would make [[range.begin() / MORSEL_SIZE]] a non-correct
        /// chunk position.
        /// This would yield wrong results.
        tbb::blocked_range<size_t> chunks_range(0, num_thread_chunks);

        tbb::parallel_for(chunks_range,
                [&](tbb::blocked_range<size_t>& chunks_range) {
            for (size_t chunks_idx = chunks_range.begin(); chunks_idx != chunks_range.end(); ++chunks_idx) {
                size_t start = chunks_idx * MORSEL_SIZE;
                size_t end = std::min(chunks_idx * MORSEL_SIZE + MORSEL_SIZE,
                                      partition_bin.size());
                LeftEntry *last_match = nullptr;

                for (size_t i = end; i != start; --i) {
                    if (partition_bin[i - 1].matched) {
                        last_match = &partition_bin[i - 1];
                        break;
                    }
                }
                size_t morsel_pos = start / MORSEL_SIZE;
                last_match_per_range[morsel_pos] = last_match;
            }
        });

        tbb::parallel_for(chunks_range,
                [&](tbb::blocked_range<size_t>& chunks_range) {
            for (size_t chunks_idx = chunks_range.begin(); chunks_idx != chunks_range.end(); ++chunks_idx) {
                size_t start = chunks_idx * MORSEL_SIZE;
                size_t end = std::min(chunks_idx * MORSEL_SIZE + MORSEL_SIZE,
                                      partition_bin.size());
                LeftEntry *last_match = nullptr;

                size_t morsel_pos = start / MORSEL_SIZE;
                for (size_t i = morsel_pos; i != 0; --i) {
                    if (last_match_per_range[i - 1] != nullptr) {
                        last_match = last_match_per_range[i - 1];
                        break;
                    }
                }

                for (size_t i = start; i != end; ++i) {
                    auto &entry = partition_bin[i];
                    if (entry.matched) {
                        last_match = &entry;
                    }

                    if (last_match && last_match->matched) {
                        result.insert(
                                /* price_timestamp= */ prices.timestamps[last_match->price_idx],
                                /* price_stock_id= */ prices.stock_ids[last_match->price_idx],
                                /// Price if locking was used.
                                ///* price= */ prices.prices[last_match->price_idx],
                                /// Price if CAS was used.
                                /* price= */ prices.prices[last_match->diff_price.load().price_idx],
                                /* order_book_timestamp= */ order_book.timestamps[entry.order_idx],
                                /* order_book_stock_id= */ order_book.stock_ids[entry.order_idx],
                                /* amount= */ order_book.amounts[entry.order_idx]);
                    }
                }
            }
        });
    });
    e.stopCounters();
    log("\n\nFinding Match Perf:");
    log(e.getReport(order_book.size));
    log(fmt::format("Finding match in {}{}", timer.lap(), timer.unit()));

    // Print lock contention duration.
    //size_t total_duration = 0;
    //for (auto& [_, entries] : order_book_lookup) {
    //    for (auto& entry : entries) {
    //        //std::cout << "Contended for " << entry.lock.get_contended_duration_ns() << std::endl;
    //        total_duration += entry.lock.get_contended_duration_ns();
    //    }
    //}
    //std::cout << "Total contention duration: " << total_duration << std::endl;

    result.finalize();
}
