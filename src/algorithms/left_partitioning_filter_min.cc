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
    /// Use alignas(64) to prevent false sharing.
    //alignas(64) std::atomic<uint64_t> global_min = 0;
    uint64_t global_min = 0;
    alignas(64) std::atomic<uint64_t> num_lookups_skipped = 0;
    alignas(64) std::atomic<bool> update_lock_flag = false;

    tbb::parallel_for(tbb::blocked_range<size_t>(0, prices.size, MORSEL_SIZE),
            [&](tbb::blocked_range<size_t>& range) {
        bool possible_new_min = false;
        size_t local_num_skipped = 0;

        uint64_t local_global_min{global_min};
        for (size_t i = range.begin(); i < range.end(); ++i) {
            auto timestamp = prices.timestamps[i];
            if (timestamp < local_global_min) {
                #ifndef BENCHMARK_MODE
                    ++local_num_skipped;
                #endif
                continue;
            }

            const auto& stock_id = prices.stock_ids[i];
            if (!order_book_lookup.contains(stock_id)) {
                continue;
            }

            possible_new_min = true;

            auto& partition_bin = order_book_lookup[stock_id];
            auto* match= Search::Interpolation::greater_equal_than(
                /* data= */ partition_bin,
                /* target= */ timestamp);

            if (match != nullptr) {
                uint64_t diff = match->timestamp - timestamp;
                match->lock_compare_swap_diffs(diff, i);
            }
        }
        #ifndef BENCHMARK_MODE
          num_lookups_skipped += local_num_skipped;
        #endif

        if (!possible_new_min) {
            /// Can skip updates.
            return;
        }

        /// Assert that only one thread updates the global min.
        bool expected_lock_flag = false;
        if (!update_lock_flag.compare_exchange_weak(
                expected_lock_flag,
                true,
                std::memory_order_acquire)) {
            return;
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
                : prices.timestamps[partition[0].price_idx]);
        }

        /// Use CAS to tighten the global minimum if the current minimum is greater.
        if (all_partitions_have_match && local_min > global_min) {
            global_min = local_min;
            //uint64_t old_global_min = global_min.load(std::memory_order_relaxed);
            //
            //while (local_min > global_min) {
            //    if (global_min.compare_exchange_weak(
            //            /* expected= */ old_global_min,
            //            /* desired= */ local_min,
            //            /* success= */ std::memory_order_acquire,
            //            /* failure= */ std::memory_order_relaxed)) { break; }
            //}
        }

        update_lock_flag = false;
    });
    e.stopCounters();
    log("\n\nBinary Search Perf:");
    log(e.getReport(prices.size));
    log(fmt::format("Binary Search in {}{}", timer.lap(), timer.unit()));
    log(fmt::format("Num lookups skipped: {}", num_lookups_skipped.load()));
    log(fmt::format("Num lookups skipped (%): {}",
                    static_cast<double>(num_lookups_skipped.load()) / prices.size));

    e.startCounters();
    tbb::parallel_for_each(order_book_lookup.begin(), order_book_lookup.end(),
            [&](auto& iter) {
        std::vector<LeftEntry>& partition_bin = iter.second;
        const size_t num_thread_chunks = (partition_bin.size() + MORSEL_SIZE - 1) / MORSEL_SIZE;
        std::vector<LeftEntry*> last_match_per_range(num_thread_chunks, nullptr);

        /// We have to parallel iterate over the number of thread chunks since TBB is not forced to align
        /// each chunk to [[MORSEL_SIZE]] which would make [[range.begin() / MORSEL_SIZE]] a non-correct
        /// chunk position.
        /// This would yield wrong results.
        tbb::blocked_range<size_t> chunks_range(0, num_thread_chunks);

        tbb::parallel_for(chunks_range,
                [&](tbb::blocked_range<size_t>& chunks_range) {
            for (size_t chunks_idx = chunks_range.begin(); chunks_idx != chunks_range.end(); ++chunks_idx) {
                size_t start = chunks_idx * MORSEL_SIZE;
                size_t end = std::min(start + MORSEL_SIZE, partition_bin.size());

                for (size_t i = end; i != start; --i) {
                    if (partition_bin[i - 1].matched) {
                        last_match_per_range[chunks_idx] = &partition_bin[i - 1];
                        break;
                    }
                }
            }
        });

        /// Calculate prefix sum of last match per chunk
        for (size_t i = 1; i < num_thread_chunks; ++i) {
            if (!last_match_per_range[i]) {
                last_match_per_range[i] = last_match_per_range[i - 1];
            }
        }

        tbb::parallel_for(chunks_range,
                [&](tbb::blocked_range<size_t>& chunks_range) {
            for (size_t chunks_idx = chunks_range.begin(); chunks_idx != chunks_range.end(); ++chunks_idx) {
                size_t start = chunks_idx * MORSEL_SIZE;
                size_t end = std::min(start + MORSEL_SIZE, partition_bin.size());
                LeftEntry *last_match = chunks_idx == 0 ? nullptr : last_match_per_range[chunks_idx - 1];

                //for (size_t i = chunks_idx; i != 0; --i) {
                //    if (last_match_per_range[i - 1] != nullptr) {
                //        last_match = last_match_per_range[i - 1];
                //        break;
                //    }
                //}

                for (size_t i = start; i != end; ++i) {
                    auto &entry = partition_bin[i];
                    if (entry.matched) {
                        last_match = &entry;
                    }

                    if (last_match) {
                        result.insert(
                            /* price_timestamp= */ prices.timestamps[last_match->price_idx],
                            /* price_stock_id= */ prices.stock_ids[last_match->price_idx],
                            /* price= */ prices.prices[last_match->price_idx],
                            ///* price= */ prices.prices[last_match->diff_price.load().price_idx],
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
