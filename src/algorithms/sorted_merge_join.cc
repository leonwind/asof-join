#include "asof_join.hpp"
#include "timer.hpp"
#include "log.hpp"
#include <unordered_map>
#include <cassert>
#include <fmt/format.h>
#include "tbb/parallel_sort.h"


void SortingASOFJoin::join() {
    Timer timer;
    timer.start();

    std::vector<size_t> prices_indices(prices.size);
    for (size_t i = 0; i < prices.size; ++i) { prices_indices[i] = i; }
    tbb::parallel_sort(prices_indices.begin(), prices_indices.end(),
        [&](size_t i, size_t j) {
        return prices.stock_ids[i] != prices.stock_ids[j]
            ? prices.stock_ids[i] < prices.stock_ids[j]
            : prices.timestamps[i] < prices.timestamps[j];
    });

    log(fmt::format("Sorted prices in {}", timer.lap()));

    std::vector<size_t> order_book_indices(order_book.size);
    for (size_t i = 0; i < order_book.size; ++i) { order_book_indices[i] = i; }
    tbb::parallel_sort(order_book_indices.begin(), order_book_indices.end(),
        [&](size_t i, size_t j) {
        return order_book.stock_ids[i] != order_book.stock_ids[j]
            ? order_book.stock_ids[i] < order_book.stock_ids[j]
            : order_book.timestamps[i] < order_book.timestamps[j];
    });

    log(fmt::format("Sorted order book in {}", timer.lap()));

    size_t i = 0, j = 0;
    while (i < prices.size && j < order_book.size) {
        size_t price_idx = prices_indices[i];
        size_t order_book_idx = order_book_indices[j];

        if (prices.stock_ids[price_idx] < order_book.stock_ids[order_book_idx]) {
            ++i;
            continue;
        } else if (prices.stock_ids[price_idx] > order_book.stock_ids[order_book_idx]) {
            ++j;
            continue;
        }

        assert(prices.stock_ids[price_idx] == order_book.stock_ids[order_book_idx]);

        size_t last_match = i;
        bool found_match = false;
        while (i < prices.size &&
            prices.stock_ids[price_idx] == order_book.stock_ids[order_book_idx] &&
            prices.timestamps[price_idx] <= order_book.timestamps[order_book_idx]) {

            last_match = i;
            ++i;
            found_match = true;
            price_idx = prices_indices[i];
        }

        if (found_match) {
            price_idx = prices_indices[last_match];

            result.prices_timestamps.push_back(prices.timestamps[price_idx]);
            result.prices_stock_ids.push_back(prices.stock_ids[price_idx]);
            result.prices.push_back(prices.prices[price_idx]);

            result.order_book_timestamps.push_back(order_book.timestamps[order_book_idx]);
            result.order_book_stock_ids.push_back(order_book.stock_ids[order_book_idx]);
            result.amounts.push_back(order_book.amounts[order_book_idx]);

            result.values.push_back(
                prices.prices[price_idx] * order_book.amounts[order_book_idx]);
        }

        ++j;
        i = last_match;
    }

    log(fmt::format("Sorted merge join in {}", timer.lap()));

    result.finalize();
}
