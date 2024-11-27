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
    log(fmt::format("Partitioning in {}{}", timer.lap(), timer.unit()));

    using Btree = Btree<size_t, Entry>;
    std::vector<Btree> price_indices;
    price_indices.reserve(prices_lookup.size());

    tbb::parallel_for_each(prices_lookup.begin(), prices_lookup.end(),
            [&](auto& iter) {
        tbb::parallel_sort(iter.second.begin(), iter.second.end());
    });

    log(fmt::format("Sorting in {}{}", timer.lap(), timer.unit()));

    result.finalize();
}