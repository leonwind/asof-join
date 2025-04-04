#include "asof_join.hpp"
#include "timer.hpp"
#include "log.hpp"
#include "parallel_multi_map.hpp"
#include "searches.hpp"
#include <fmt/format.h>
#include <unordered_map>
#include <mutex>
#include "tbb/parallel_sort.h"
#include "tbb/parallel_for.h"
#include "tbb/parallel_for_each.h"


// Morsel size is 16384
#define MORSEL_SIZE (2<<14)

void PartitioningLeftASOFJoin::join() {
    PerfEvent e;
    Timer<milliseconds> timer;
    timer.start();

    e.startCounters();
    MultiMapTB<LeftEntry> order_book_lookup(order_book.stock_ids, order_book.timestamps);
    e.stopCounters();
    log("Partitioning Perf");
    log(e.getReport(order_book.size));
    log(fmt::format("Partitioning in {}{}", timer.lap(), timer.unit()));

    e.startCounters();
    tbb::parallel_for_each(order_book_lookup.begin(), order_book_lookup.end(),
            [&](auto& iter) {
        tbb::parallel_sort(iter.second.begin(), iter.second.end());
    });
    e.stopCounters();
    log("\n\nSorting Perf: ");
    log(e.getReport(order_book.size));
    log(fmt::format("Sorting in {}{}", timer.lap(), timer.unit()));

    e.startCounters();
    tbb::parallel_for(tbb::blocked_range<size_t>(0, prices.size, MORSEL_SIZE),
            [&](tbb::blocked_range<size_t>& range) {
        for (size_t i = range.begin(); i < range.end(); ++i) {
            auto& stock_id = prices.stock_ids[i];
            auto bin_ptr = order_book_lookup.find(stock_id);
            if (bin_ptr == nullptr) {
                continue;
            }

            auto timestamp = prices.timestamps[i];
            auto* match = Search::Interpolation::greater_equal_than(
                /* data= */ *bin_ptr,
                /* target= */ timestamp);

            if (match != nullptr) {
                uint64_t diff = match->timestamp - timestamp;
                match->lock_compare_swap_diffs(diff, i);
                //match->atomic_compare_swap_diffs(diff, i);
            }
        }
    });
    e.stopCounters();
    log("\n\nBinary Search Perf:");
    log(e.getReport(prices.size));
    log(fmt::format("Binary Search in {}{}", timer.lap(), timer.unit()));

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
