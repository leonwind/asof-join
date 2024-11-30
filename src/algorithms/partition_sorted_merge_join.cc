#include "asof_join.hpp"
#include "timer.hpp"
#include "log.hpp"
#include "parallel_multi_map.hpp"
#include <fmt/format.h>
#include <unordered_map>
#include <mutex>
#include "tbb/parallel_sort.h"
#include "tbb/parallel_for_each.h"


// Morsel size is 16384
#define MORSEL_SIZE (2<<14)

void PartitioningSortedMergeJoin::join() {
    PerfEvent e;
    Timer<milliseconds> timer;
    timer.start();

    e.startCounters();
    MultiMap<Entry> prices_index(prices.stock_ids, prices.timestamps);
    log(fmt::format("Left Partitioning in {}{}", timer.lap(), timer.unit()));

    MultiMap<Entry> order_book_index(order_book.stock_ids, order_book.timestamps);
    log(fmt::format("Right Partitioning in {}{}", timer.lap(), timer.unit()));
    std::cout << "\n\nPartitioning Perf" << std::endl;
    e.stopCounters();
    e.printReport(std::cout, order_book.size + prices.size);

    e.startCounters();
    tbb::parallel_for_each(prices_index.begin(), prices_index.end(),
            [&](auto& iter) {
        tbb::parallel_sort(iter.second.begin(), iter.second.end());
    });

    tbb::parallel_for_each(order_book_index.begin(), order_book_index.end(),
            [&](auto& iter) {
        tbb::parallel_sort(iter.second.begin(), iter.second.end());
    });
    e.stopCounters();
    log("\n\nSorting Perf");
    e.printReport(std::cout,  order_book.size + prices.size);
    log(fmt::format("Sorting in {}{}", timer.lap(), timer.unit()));

    e.startCounters();
    std::mutex result_lock;
    tbb::parallel_for_each(order_book_index.begin(), order_book_index.end(),
            [&](auto& iter) {
        const std::vector<Entry>& orders_bin = order_book_index[iter.first];

        if (!prices_index.contains(iter.first)) {
            return;
        }
        const std::vector<Entry>& prices_bin = prices_index[iter.first];

        size_t l = 0;
        size_t r = 0;

        while(l < orders_bin.size() && r < prices_bin.size()) {
            bool found_match = false;
            size_t last_valid_r = r;

            while(r < prices_bin.size() &&
                prices_bin[r].timestamp <= orders_bin[l].timestamp) {
                found_match = true;
                last_valid_r = r;
                ++r;
            }

            if (found_match) {
                size_t order_idx = orders_bin[l].idx;
                size_t price_idx = prices_bin[last_valid_r].idx;

                std::scoped_lock lock{result_lock};
                result.insert(
                    /* price_timestamp= */ prices.timestamps[price_idx],
                    /* price_stock_id= */ prices.stock_ids[price_idx],
                    /* price= */ prices.prices[price_idx],
                    /* order_book_timestamp= */ order_book.timestamps[order_idx],
                    /* order_book_stock_id= */ order_book.stock_ids[order_idx],
                    /* amount= */ order_book.amounts[order_idx]);
            }

            ++l;
            r = last_valid_r;
        }
    });
    e.stopCounters();
    log("Sorted Merge Join Perf");
    e.printReport(std::cout, order_book.size + prices.size);

    log(fmt::format("Partitioned Sorted Merge join in {}{}", timer.lap(), timer.unit()));

    result.finalize();
}
