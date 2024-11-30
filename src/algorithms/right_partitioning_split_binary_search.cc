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

using JoinAlg = PartitioningRightSplitBinarySearchASOFJoin;

inline PartitioningRightASOFJoin::Entry* PartitioningRightASOFJoin::binary_search_closest_match_greater_than(
        std::vector<Entry> &data, uint64_t target) {
    auto iter = std::upper_bound(data.begin(), data.end(), target,
        [](uint64_t a, const Entry &b) {
            return a <= b.timestamp;
    });

    if (iter == data.end()) {
        return nullptr;
    }

    return &(*iter);
}

namespace {
    size_t subset_binary_search_closest_match(
            std::vector<JoinAlg::Entry>& data,
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
            [](uint64_t a, const JoinAlg::Entry &b) {
                return a <= b.timestamp;
        });

        return iter != end_iter
            ? iter - data.begin()
            : UINT64_MAX;
    }

    struct LocalPartition {
        std::vector<std::string_view> stock_ids;
        std::vector<uint64_t> timestamps;
    };
} // namespace

void JoinAlg::join() {
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


    std::unordered_map<std::string_view, std::pair<size_t, size_t>> bs_offsets(order_book_lookup.size());
    for (auto& iter : order_book_lookup) {
        bs_offsets[iter.first] = {UINT64_MAX, UINT64_MAX};
    }
    tbb::enumerable_thread_specific<std::unordered_map<std::string_view, std::pair<size_t, size_t>>>
        thread_bs_offsets{bs_offsets};

    tbb::parallel_for(tbb::blocked_range<size_t>(0, prices.size, MORSEL_SIZE),
            [&](tbb::blocked_range<size_t>& range) {

        auto& local_bs_offsets = thread_bs_offsets.local();

        for (size_t i = range.begin(); i < range.end(); ++i) {
            const auto& stock_id = prices.stock_ids[i];
            if (!order_book_lookup.contains(stock_id)) {
                continue;
            }
            uint64_t last_target = local_bs_offsets[stock_id].first;
            size_t last_pos = local_bs_offsets[stock_id].second;

            size_t start_offset = 0;
            size_t end_offset = 0;

            auto& partition_bin = order_book_lookup[stock_id];
            auto timestamp = prices.timestamps[i];
            //std::cout << "\n\nLast pos: " << last_pos << ", last target: "
            //    << last_target << ", target: " << timestamp << std::endl;

            /**
             * [1, 2, 3, 4, 5]
             * last_pos = 1 => last_target = 2
             * start_off = 1 * (1) = 1
             * end_off = (5 - 1) * 1 = 4
             */

            if (last_target != UINT64_MAX && last_pos != 0) [[likely]] {
                start_offset = last_pos * (timestamp >= last_target);
                end_offset = (partition_bin.size() - (last_pos + 1)) * (timestamp <= last_target);
            }

            if (start_offset >= partition_bin.size() || end_offset >= partition_bin.size()) {
                std::cout << "SOMETHING IS WRONG???" << std::endl;
                std::cout << "\n\nLast pos: " << last_pos << ", last target: "
                    << last_target << ", target: " << timestamp << std::endl;
                std::cout << "Start offset: " << start_offset << ", end offset: " << end_offset
                        << "size: " << partition_bin.size() << std::endl;
            }

            auto entry_pos = subset_binary_search_closest_match(
                partition_bin,
                timestamp,
                start_offset,
                end_offset);

            local_bs_offsets[stock_id] = {timestamp, entry_pos == UINT64_MAX ? 0 : entry_pos};

            if (entry_pos == UINT64_MAX) {
                continue;
            }
            auto& match = partition_bin[entry_pos];

            uint64_t diff = match.timestamp - timestamp;
            /// (Spin) Lock while comparing and exchanging the diffs
            /// CAS is probably too expensive on two 64 Bit fields.
            match.lock.lock();
            if (diff < match.diff) {
                match.diff = diff;
                match.price_idx = i;
            }
            match.lock.unlock();

            match.matched = true;
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
    e.stopCounters();
    std::cout << "\n\nFinding Match Perf: " << std::endl;
    e.printReport(std::cout, prices.size);
    log(fmt::format("Finding match in {}{}", timer.lap(), timer.unit()));

    result.finalize();
}
