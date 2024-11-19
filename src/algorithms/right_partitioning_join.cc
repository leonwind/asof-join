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

inline std::optional<size_t> PartitioningRightASOFJoin::binary_search_closest_match_greater_than(
        const std::vector<PartitioningRightASOFJoin::Entry> &data, uint64_t target) {
    auto iter = std::upper_bound(data.begin(), data.end(), target,
        [](uint64_t a, const PartitioningRightASOFJoin::Entry &b) {
            return a <= b.timestamp;
    });

    if (iter == data.end()) {
        return {};
    }

    return {iter - data.begin()};
}

void PartitioningRightASOFJoin::join() {
    PerfEvent e;
    Timer<milliseconds> timer;
    timer.start();

    e.startCounters();
    MultiMap<Entry> order_book_lookup(order_book.stock_ids, order_book.timestamps);
    e.stopCounters();
    log("Partitioning Perf");
    e.printReport(std::cout, order_book.size);
    log(fmt::format("Partitioning in {}{}", timer.lap(), timer.unit()));

    e.startCounters();
    tbb::parallel_for_each(order_book_lookup.begin(), order_book_lookup.end(),
            [&](auto& iter) {
        tbb::parallel_sort(iter.second.begin(), iter.second.end());
    });
    e.stopCounters();
    log("\n\nSorting Perf: ");
    e.printReport(std::cout, prices.size);
    log(fmt::format("Sorting in {}{}", timer.lap(), timer.unit()));

    e.startCounters();
    tbb::parallel_for(tbb::blocked_range<size_t>(0, prices.size, MORSEL_SIZE),
            [&](tbb::blocked_range<size_t>& range) {
        for (size_t i = range.begin(); i < range.end(); ++i) {
            const auto& stock_id = prices.stock_ids[i];
            auto& timestamp = prices.timestamps[i];

            if (!order_book_lookup.contains(stock_id)) {
                continue;
            }
            auto& partition_bin = order_book_lookup[stock_id];

            auto match_idx_opt= binary_search_closest_match_greater_than(
                /* data= */ partition_bin,
                /* target= */ timestamp);

            if (match_idx_opt.has_value()) {
                size_t match_idx = match_idx_opt.value();
                uint64_t diff = partition_bin[match_idx].timestamp - timestamp;

                // Lock while comparing and exchanging the diffs
                partition_bin[match_idx].lock.lock();
                if (diff < partition_bin[match_idx].diff) {
                    partition_bin[match_idx].diff = diff;
                    partition_bin[match_idx].price_idx = i;
                }
                partition_bin[match_idx].lock.unlock();

                partition_bin[match_idx].matched = true;
            }
        }
    });
    e.stopCounters();
    std::cout << "\n\nBinary Search Perf: " << std::endl;
    e.printReport(std::cout, prices.size);
    log(fmt::format("Binary Search in {}{}", timer.lap(), timer.unit()));

    e.startCounters();
    std::mutex result_lock;
    tbb::parallel_for_each(order_book_lookup.begin(), order_book_lookup.end(),
            [&](auto& iter) {
        std::vector<Entry>& partition_bin = iter.second;
        const size_t num_thread_chunks = (partition_bin.size() + MORSEL_SIZE - 1) / MORSEL_SIZE;
        std::vector<Entry*> last_match_per_range(num_thread_chunks);

        tbb::parallel_for(tbb::blocked_range<size_t>(0, partition_bin.size(), MORSEL_SIZE),
                [&](tbb::blocked_range<size_t>& range) {
            Entry* last_match = nullptr;
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
            Entry* last_match = nullptr;
            size_t morsel_pos = range.begin() / MORSEL_SIZE;
            for (size_t i = morsel_pos; i != 0; --i) {
                if (last_match_per_range[i] != nullptr) {
                    last_match = last_match_per_range[i];
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
    e.stopCounters();
    std::cout << "\n\nFinding Match Perf: " << std::endl;
    e.printReport(std::cout, prices.size);
    log(fmt::format("Finding match in {}{}", timer.lap(), timer.unit()));

    result.finalize();
}
