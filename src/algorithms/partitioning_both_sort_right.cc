#include "asof_join.hpp"
#include "timer.hpp"
#include "log.hpp"
#include "util.hpp"
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

using JoinAlg = PartitioningBothSortLeftASOFJoin;

inline JoinAlg::LeftEntry* JoinAlg::binary_search_closest_match_greater_than(
        std::vector<LeftEntry> &data, uint64_t target) {
    auto iter = std::lower_bound(data.begin(), data.end(), target,
        [](const LeftEntry &b, uint64_t a) {
            return b.timestamp < a;
    });

    if (iter == data.end()) {
        return nullptr;
    }

    return &(*iter);
}

namespace {
    size_t subset_binary_search_closest_match(
            std::vector<JoinAlg::LeftEntry>& data,
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
            [](uint64_t a, const JoinAlg::LeftEntry &b) {
                return a <= b.timestamp;
        });

        return iter != end_iter
            ? iter - data.begin()
            : UINT64_MAX;
    }
} // namespace

void JoinAlg::join() {
    PerfEvent e;
    Timer<milliseconds> timer;
    timer.start();

    //e.startCounters();
    MultiMapTB<LeftEntry> order_book_lookup(order_book.stock_ids, order_book.timestamps);
    log(fmt::format("Right Partitioning in {}{}", timer.lap(), timer.unit()));

    //MultiMapTB<RightEntry> prices_lookup(prices.stock_ids, prices.timestamps);
    //log(fmt::format("Left Partitioning in {}{}", timer.lap(), timer.unit()));
    //e.stopCounters();
    //log("Partitioning Perf");
    //e.printReport(std::cout, order_book.size);

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

    const uint32_t num_partitions = 128;
    const uint32_t mask = num_partitions - 1;
    std::hash<std::string> hash;

    tbb::parallel_for(tbb::blocked_range<size_t>(0, prices.size, MORSEL_SIZE),
            [&](tbb::blocked_range<size_t>& range) {
        /// Partition the current batch to keep the caches hot since we can run multiple
        /// binary searches on the same data after each other.
        std::vector<TupleBuffer<RightEntry>> partitions(num_partitions);
        for (size_t i = range.begin(); i < range.end(); ++i) {
            size_t pos = hash(prices.stock_ids[i]) & mask;
            partitions[pos].emplace_back(
                /* timestamp= */ prices.timestamps[i],
                /* idx= */ i);
        }

        tbb::parallel_for_each(partitions.begin(), partitions.end(),
            [&](auto& partition) {
            //auto partition = tb_partition.copy_tuples();
            //tbb::parallel_sort(partition.begin(), partition.end());

            for (auto& entry : partition) {
                const auto& stock_id = prices.stock_ids[entry.idx];
                if (!order_book_lookup.contains(stock_id)) {
                    continue;
                }

                auto& partition_bin = order_book_lookup[stock_id];
                auto timestamp = entry.timestamp;
                auto* match = binary_search_closest_match_greater_than(
                    /* data= */ partition_bin,
                    /* target= */ timestamp);

                if (match != nullptr) {
                    uint64_t diff = match->timestamp - timestamp;

                    /// (Spin) Lock while comparing and exchanging the diffs.
                    /// CAS is probably too expensive on two 64 Bit fields.
                    match->lock.lock();
                    if (diff < match->diff) {
                        match->diff = diff;
                        match->price_idx = entry.idx;
                    }
                    match->lock.unlock();

                    match->matched = true;
                }
            }
        });
    });
    e.stopCounters();
    log("\n\nBinary Search + Partitioning Perf: ");
    log(e.getReport(prices.size));
    log(fmt::format("Binary Search + Partitioning in {}{}", timer.lap(), timer.unit()));

    //e.startCounters();
    tbb::parallel_for_each(order_book_lookup.begin(), order_book_lookup.end(),
            [&](auto& iter) {
        std::vector<LeftEntry>& partition_bin = iter.second;
        const size_t num_thread_chunks = (partition_bin.size() + MORSEL_SIZE - 1) / MORSEL_SIZE;
        std::vector<LeftEntry*> last_match_per_range(num_thread_chunks);

        /// We have to parallel iterate over the number of thread chunks since TBB is not forced to align
        /// each chunk to [[MORSEL_SIZE]] which would make [[range.begin() / MORSEL_SIZE]] a non-correct
        /// chunk position. This would yield wrong results.
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
    log("\n\nFinding Match Perf: ");
    log(e.getReport(prices.size));
    log(fmt::format("Finding match in {}{}", timer.lap(), timer.unit()));

    result.finalize();
}
