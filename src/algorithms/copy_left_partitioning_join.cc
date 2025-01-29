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

void PartitioningLeftCopyLeftASOFJoin::join() {
    PerfEvent e;
    Timer<milliseconds> timer;
    timer.start();

    //e.startCounters();
    const size_t num_threads = tbb::this_task_arena::max_concurrency();
    MultiMapTB<LeftEntryCopy> order_book_lookup(order_book.stock_ids, order_book.timestamps);

    log("Partitioning Perf");
    //e.printReport(std::cout, order_book.size);
    log(fmt::format("Partitioning in {}{}", timer.lap(), timer.unit()));

    //e.startCounters();
    tbb::parallel_for_each(order_book_lookup.begin(), order_book_lookup.end(),
            [&](auto& iter) {
        tbb::parallel_sort(iter.second.begin(), iter.second.end());
    });

    std::vector<MultiMapTB<LeftEntryCopy>> order_book_lookups(num_threads, order_book_lookup);

    //e.stopCounters();
    log("\n\nSorting Perf: ");
    //e.printReport(std::cout, prices.size);
    log(fmt::format("Sorting in {}{}", timer.lap(), timer.unit()));

    e.startCounters();
    tbb::parallel_for(tbb::blocked_range<size_t>(0, prices.size, MORSEL_SIZE),
            [&](tbb::blocked_range<size_t>& range) {
        const size_t thread_id = tbb::this_task_arena::current_thread_index();
        auto& local_order_book_lookup = order_book_lookups[thread_id];
        //auto& local_order_book_lookup = order_book_lookups_tbb.local();

        for (size_t i = range.begin(); i < range.end(); ++i) {
            const auto& stock_id = prices.stock_ids[i];
            if (!local_order_book_lookup.contains(stock_id)) {
                continue;
            }

            auto& partition_bin = local_order_book_lookup[stock_id];
            auto timestamp = prices.timestamps[i];
            auto* match= Search::Interpolation::greater_equal_than(
                /* data= */ partition_bin,
                /* target= */ timestamp);

            if (match != nullptr) {
                uint64_t diff = match->timestamp - timestamp;
                if (diff < match->diff) {
                    match->diff = diff;
                    match->price_idx = i;
                    match->matched = true;
                }
            }
        }
    });
    e.stopCounters();
    log("\n\nBinary Search Perf:");
    log(e.getReport(prices.size));
    log(fmt::format("Binary Search in {}{}", timer.lap(), timer.unit()));

    e.startCounters();
    /// Merge all thread local order book lookups into the global [[order_book_lookup]].
    /// For all stock_ids iterate over all partitions and copy for a thread-local range
    /// the local matched values, if the [[diff]] is smaller, into [[order_book_lookup]].
    tbb::parallel_for_each(order_book_lookup.begin(), order_book_lookup.end(),
            [&](auto& s_id_bin) {
        auto& s_id = s_id_bin.first;
        auto& global_bin = s_id_bin.second;

        tbb::parallel_for(tbb::blocked_range<size_t>(0, global_bin.size()),
            [&](tbb::blocked_range<size_t>& range) {
            for (auto& local_order_book : order_book_lookups) {
                auto& local_bin = local_order_book[s_id];
                for (size_t i = range.begin(); i < range.end(); ++i) {
                    if (local_bin[i].matched && local_bin[i].diff < global_bin[i].diff) {
                        global_bin[i].diff = local_bin[i].diff;
                        global_bin[i].price_idx = local_bin[i].price_idx;
                        global_bin[i].matched = true;
                    }
                }
            }
        });
    });
    e.stopCounters();
    log("\n\nMerging Perf:");
    log(e.getReport(order_book.size));
    log(fmt::format("Merging in {}{}", timer.lap(), timer.unit()));


    e.startCounters();
    tbb::parallel_for_each(order_book_lookup.begin(), order_book_lookup.end(),
            [&](auto& iter) {
        std::vector<LeftEntryCopy>& partition_bin = iter.second;
        const size_t num_thread_chunks = (partition_bin.size() + MORSEL_SIZE - 1) / MORSEL_SIZE;
        std::vector<LeftEntryCopy*> last_match_per_range(num_thread_chunks);

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
                LeftEntryCopy *last_match = nullptr;

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
                LeftEntryCopy *last_match = nullptr;

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
                                /* price= */ prices.prices[last_match->price_idx],
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

    result.finalize();
}
