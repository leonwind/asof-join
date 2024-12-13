#include "asof_join.hpp"
#include "timer.hpp"
#include "log.hpp"
#include "parallel_multi_map.hpp"
#include "btree.hpp"
#include <fmt/core.h>
#include <unordered_map>
#include <mutex>
#include "tbb/parallel_for.h"
#include "tbb/parallel_for_each.h"
#include "tbb/parallel_sort.h"


// Morsel size is 16384
#define MORSEL_SIZE (2<<14)


void PartitioningLeftBTreeASOFJoin::join() {
    Timer<milliseconds> timer;
    timer.start();

    MultiMap<Entry> prices_lookup(prices.stock_ids, prices.timestamps);
    //log(fmt::format("Partitioning in {}{}", timer.lap(), timer.unit()));

    tbb::parallel_for_each(prices_lookup.begin(), prices_lookup.end(),
            [&](auto& iter) {
        tbb::parallel_sort(iter.second.begin(), iter.second.end());
    });
    //log(fmt::format("Sorting in {}{}", timer.lap(), timer.unit()));

    using Btree = Btree<Entry>;
    std::unordered_map<std::string_view, Btree> price_trees(prices_lookup.size());
    for (auto& stock_prices : prices_lookup) {
        auto tree = Btree(stock_prices.second);
        price_trees.insert({stock_prices.first, tree});
    }
    //log(fmt::format("Inserting into BTree in {}{}", timer.lap(), timer.unit()));

    tbb::parallel_for(tbb::blocked_range<size_t>(0, order_book.size, MORSEL_SIZE),
            [&](tbb::blocked_range<size_t>& range) {
        for (size_t i = range.begin(); i < range.end(); ++i) {
            auto& stock_id = order_book.stock_ids[i];
            if (!price_trees.contains(stock_id)) {
                continue;
            }

            auto& btree = price_trees.at(stock_id);
            auto timestamp = order_book.timestamps[i];
            auto* match = btree.find_less_equal_than(timestamp);

            if (match != nullptr) {
                result.insert(
                    /* price_timestamp= */match->timestamp,
                    /* price_stock_id= */ stock_id,
                    /* price= */ prices.prices[match->idx],
                    /* order_book_timestamp= */ timestamp,
                    /* order_book_stock_id= */ order_book.stock_ids[i],
                    /* amount= */ order_book.amounts[i]);
            }
        }
    });
    //log(fmt::format("BTree lookups in {}{}", timer.lap(), timer.unit()));

    result.finalize();
}