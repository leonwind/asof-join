#include "asof_join.hpp"
#include "timer.hpp"
#include "log.hpp"
#include "parallel_multi_map.hpp"
#include "btree.hpp"
#include <fmt/format.h>
#include <unordered_map>
#include <mutex>
#include "tbb/parallel_sort.h"
#include "tbb/parallel_for.h"
#include "tbb/parallel_for_each.h"


// Morsel size is 16384
#define MORSEL_SIZE (2<<14)

uint64_t PartitioningRightBTreeASOFJoin::join() {
    PerfEvent e;
    Timer<milliseconds> timer;
    timer.start();

    //e.startCounters();
    MultiMap<Entry> order_book_lookup(order_book.stock_ids, order_book.timestamps);
    //e.stopCounters();
    //log("Partitioning Perf");
    //e.printReport(std::cout, order_book.size);
    //log(fmt::format("Partitioning in {}{}", timer.lap(), timer.unit()));

    //e.startCounters();
    tbb::parallel_for_each(order_book_lookup.begin(), order_book_lookup.end(),
        [&](auto &iter) {
           tbb::parallel_sort(iter.second.begin(), iter.second.end());
    });

    //e.stopCounters();
    //log("\n\nSorting Perf: ");

    using Btree = Btree<Entry>;
    std::unordered_map<std::string_view, Btree> order_trees(order_book_lookup.size());
    for (auto& iter : order_book_lookup) {
        order_trees.insert({iter.first, Btree(iter.second)});
    }
    //log(fmt::format("Inserting into BTree in {}{}", timer.lap(), timer.unit()));

    tbb::parallel_for(tbb::blocked_range<size_t>(0, prices.size, MORSEL_SIZE),
            [&](tbb::blocked_range<size_t>& range) {
        for (size_t i = range.begin(); i < range.end(); ++i) {
            const auto& stock_id = prices.stock_ids[i];
            if (!order_book_lookup.contains(stock_id)) {
                continue;
            }

            if (!order_trees.contains(stock_id)) {
                std::cout << "Stock id " << stock_id << "in trees but not in index" << std::endl;
            }

            auto& tree = order_trees[stock_id];
            auto& timestamp = prices.timestamps[i];
            auto* match = tree.find_greater_equal_than(timestamp);
            if (match != nullptr) {
                uint64_t diff = match->timestamp - timestamp;
                /// (Spin) Lock while comparing and exchanging the diffs.
                /// CAS is (probably) too expensive on two 64 Bit fields.
                match->lock.lock();
                if (diff < match->diff) {
                    match->diff = diff;
                    match->price_idx = i;
                }
                match->lock.unlock();

                match->matched = true;
            }
        }
    });
    //e.stopCounters();
    //std::cout << "\n\nBTree Lookup Perf: " << std::endl;
    //e.printReport(std::cout, prices.size);
    //log(fmt::format("BTree Lookups in {}{}", timer.lap(), timer.unit()));

    //e.startCounters();
    std::mutex result_lock;
    tbb::parallel_for_each(order_trees.begin(), order_trees.end(),
            [&](auto& iter) {
        Btree& tree = iter.second;
        const size_t num_thread_chunks = tree.num_leaves();
        std::vector<Entry*> last_match_per_range(num_thread_chunks);

        tbb::parallel_for(tbb::blocked_range<size_t>(0, num_thread_chunks, 1),
                [&](tbb::blocked_range<size_t>& range) {
            size_t leave_idx = range.begin();
            auto& leaf_data = tree[leave_idx];
            Entry* last_match = nullptr;
            for (size_t i = leaf_data.size(); i != 0; --i) {
                if (leaf_data[i - 1].matched) {
                    last_match = &leaf_data[i - 1];
                    break;
                }
            }
            last_match_per_range[leave_idx] = last_match;
        });

        tbb::parallel_for(tbb::blocked_range<size_t>(0, num_thread_chunks, 1),
                [&](tbb::blocked_range<size_t>& range) {
            size_t leave_idx = range.begin();
            auto& leaf_data = tree[leave_idx];
            Entry* last_match = nullptr;
            for (size_t i = leave_idx; i != 0; --i) {
                if (last_match_per_range[i - 1] != nullptr) {
                    last_match = last_match_per_range[i - 1];
                    break;
                }
            }

            for (auto& entry : leaf_data) {
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
