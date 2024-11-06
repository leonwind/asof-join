#include "asof_join.hpp"
#include "timer.hpp"
#include "spin_lock.hpp"
#include <unordered_map>
#include <algorithm>
#include <mutex>
#include "tbb/parallel_sort.h"
#include "tbb/parallel_for.h"
#include "tbb/parallel_for_each.h"


struct Entry {
    uint64_t timestamp;
    size_t order_idx;
    size_t price_idx;
    uint64_t diff;
    bool matched;
    SpinLock lock;

    Entry(uint64_t timestamp, size_t order_idx): timestamp(timestamp), order_idx(order_idx),
        price_idx(0), diff(UINT64_MAX), matched(false), lock() {}

    Entry(const Entry& other): timestamp(other.timestamp), order_idx(other.order_idx),
        price_idx(other.price_idx), diff(other.diff), matched(other.matched), lock() {}

    Entry(Entry&& other) noexcept: timestamp(other.timestamp), order_idx(other.order_idx),
       price_idx(other.price_idx), diff(other.diff), matched(other.matched), lock() {}

    Entry& operator = (const Entry& other) {
        if (this != &other) {
            timestamp = other.timestamp;
            order_idx = other.order_idx;
            price_idx = other.price_idx;
            diff = other.diff;
            matched = other.matched;
        }
        return *this;
    }

    Entry& operator = (Entry&& other)  noexcept {
        if (this != &other) {
            timestamp = other.timestamp;
            order_idx = other.order_idx;
            price_idx = other.price_idx;
            diff = other.diff;
            matched = other.matched;
        }
        return *this;
    }

    std::strong_ordering operator <=> (const Entry& other) const {
        return timestamp <=> other.timestamp;
    }
};

std::pair<bool, size_t> binary_search_closest_match_greater_than(
        const std::vector<Entry>& data, uint64_t target) {
    size_t left = 0;
    size_t right = data.size();

    while (left < right) {
        size_t mid = left + (right - left) / 2;

        if (data[mid].timestamp < target) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    return {left < data.size() && data[left].timestamp >= target, left};
}

ResultRelation PartitioningRightASOFJoin::join() {
    ResultRelation result(prices, order_book);

    Timer timer;
    timer.start();

    std::unordered_map<std::string_view, std::vector<Entry>> order_book_lookup;
    order_book_lookup.reserve(order_book.size);

    for (size_t i = 0; i < order_book.size; ++i) {
        if (order_book_lookup.contains(order_book.stock_ids[i])) {
            order_book_lookup[order_book.stock_ids[i]].emplace_back(
                order_book.timestamps[i], i
            );
        } else {
            order_book_lookup[order_book.stock_ids[i]] = std::vector<Entry>{
                {order_book.timestamps[i], i}
            };
        }
    }

    std::cout << "Partitioning in " << timer.lap() << std::endl;

    tbb::parallel_for_each(order_book_lookup.begin(), order_book_lookup.end(),
            [&](auto& iter) {
        //std::sort(iter.second.begin(), iter.second.end());
        tbb::parallel_sort(iter.second.begin(), iter.second.end());
    });

    std::cout << "Sorting in " << timer.lap() << std::endl;

    tbb::parallel_for(tbb::blocked_range<size_t>(0, prices.size, 10000),
            [&](tbb::blocked_range<size_t>& range) {
        for (size_t i = range.begin(); i < range.end(); ++i) {
            auto& stock_id = prices.stock_ids[i];
            auto timestamp = prices.timestamps[i];

            auto& values = order_book_lookup[stock_id];
            if (values.empty()) {
                continue;
            }

            auto [found_join_partner, match_idx] =
                binary_search_closest_match_greater_than(
                    /* data= */ values,
                    /* target= */ timestamp);

            if (found_join_partner) {
                size_t order_book_idx = values[match_idx].order_idx;
                uint64_t diff = order_book.timestamps[order_book_idx] - timestamp;

                // Lock while comparing and exchanging the diffs
                values[match_idx].lock.lock();
                if (diff < values[match_idx].diff) {
                    values[match_idx].diff = diff;
                    values[match_idx].price_idx = i;
                }
                values[match_idx].lock.unlock();

                values[match_idx].matched = true;
            }
        }
    });

    std::cout << "Binary Search in " << timer.lap() << std::endl;

    std::mutex result_lock;
    tbb::parallel_for_each(order_book_lookup.begin(), order_book_lookup.end(),
            [&](auto& iter) {
        auto& values = iter.second;

        Entry* last_match = nullptr;
        for (auto& entry : values) {
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

    std::cout << "Finding match in " << timer.lap() << std::endl;

    result.finalize();
    return result;
}
