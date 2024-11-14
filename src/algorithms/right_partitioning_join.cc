#include "asof_join.hpp"
#include "timer.hpp"
#include "spin_lock.hpp"
#include "log.hpp"
#include <fmt/format.h>
#include <algorithm>
#include <unordered_map>
#include <mutex>
#include "tbb/parallel_sort.h"
#include "tbb/parallel_for.h"
#include "tbb/parallel_for_each.h"


// Morsel size is 16384
#define MORSEL_SIZE (2<<14)

namespace {
struct Entry {
    uint64_t timestamp;
    size_t order_idx;
    size_t price_idx;
    uint64_t diff;
    bool matched;
    SpinLock lock;

    Entry(uint64_t timestamp, size_t order_idx) : timestamp(timestamp), order_idx(order_idx),
                                                  price_idx(0), diff(UINT64_MAX), matched(false), lock() {}

    Entry(const Entry &other) : timestamp(other.timestamp), order_idx(other.order_idx),
                                price_idx(other.price_idx), diff(other.diff), matched(other.matched), lock() {}

    Entry(Entry &&other) noexcept: timestamp(other.timestamp), order_idx(other.order_idx),
                                   price_idx(other.price_idx), diff(other.diff), matched(other.matched), lock() {}

    Entry &operator=(const Entry &other) {
        if (this != &other) {
            timestamp = other.timestamp;
            order_idx = other.order_idx;
            price_idx = other.price_idx;
            diff = other.diff;
            matched = other.matched;
        }
        return *this;
    }

    Entry &operator=(Entry &&other) noexcept {
        if (this != &other) {
            timestamp = other.timestamp;
            order_idx = other.order_idx;
            price_idx = other.price_idx;
            diff = other.diff;
            matched = other.matched;
        }
        return *this;
    }

    std::strong_ordering operator<=>(const Entry &other) const {
        return timestamp <=> other.timestamp;
    }
};

std::optional<size_t> binary_search_closest_match_greater_than(
        const std::vector<Entry> &data, uint64_t target) {
    auto iter = std::upper_bound(data.begin(), data.end(), target,
                                 [](uint64_t a, const Entry &b) {
                                     return a <= b.timestamp;
                                 });

    if (iter == data.end()) {
        return {};
    }

    return {iter - data.begin()};
}
} // namespace

void PartitioningRightASOFJoin::join() {
    Timer timer;
    timer.start();

    std::unordered_map<std::string_view, std::vector<Entry>> order_book_lookup;
    for (size_t i = 0; i < order_book.size; ++i) {
        order_book_lookup[order_book.stock_ids[i]].emplace_back(
            /* timestamp= */ order_book.timestamps[i],
            /* order_idx= */ i);
    }

    log(fmt::format("Partitioning in {}", timer.lap()));

    tbb::parallel_for_each(order_book_lookup.begin(), order_book_lookup.end(),
            [&](auto& iter) {
        tbb::parallel_sort(iter.second.begin(), iter.second.end());
    });

    log(fmt::format("Sorting in {}", timer.lap()));

    tbb::parallel_for(tbb::blocked_range<size_t>(0, prices.size, MORSEL_SIZE),
            [&](tbb::blocked_range<size_t>& range) {
        for (size_t i = range.begin(); i < range.end(); ++i) {
            const auto& stock_id = prices.stock_ids[i];
            auto timestamp = prices.timestamps[i];

            if (!order_book_lookup.contains(stock_id)) {
                continue;
            }
            auto& values = order_book_lookup[stock_id];

            auto match_idx_opt= binary_search_closest_match_greater_than(
                /* data= */ values,
                /* target= */ timestamp);

            if (match_idx_opt.has_value()) {
                size_t match_idx = match_idx_opt.value();
                uint64_t diff = values[match_idx].timestamp - timestamp;

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

    log(fmt::format("Binary Search in {}", timer.lap()));

    std::mutex result_lock;
    tbb::parallel_for_each(order_book_lookup.begin(), order_book_lookup.end(),
            [&](auto& iter) {
        std::vector<Entry>& values = iter.second;

        std::vector<Entry*> last_match_per_range((values.size() + MORSEL_SIZE - 1) / MORSEL_SIZE);
        tbb::parallel_for(tbb::blocked_range<size_t>(0, values.size(), MORSEL_SIZE),
                [&](tbb::blocked_range<size_t>& range) {
            Entry* last_match = nullptr;
            for (size_t i = range.end() - 1; i != range.begin(); --i) {
                if (values[i].matched) {
                    last_match = &values[i];
                    break;
                }
            }

            last_match_per_range[range.begin() / MORSEL_SIZE] = last_match;
        });

        tbb::parallel_for(tbb::blocked_range<size_t>(0, values.size(), MORSEL_SIZE),
                [&](tbb::blocked_range<size_t>& range) {
            Entry* last_match = nullptr;
            for (size_t morsel_idx = range.begin() / MORSEL_SIZE; morsel_idx != 0; --morsel_idx) {
                if (last_match_per_range[morsel_idx]) {
                    last_match = last_match_per_range[morsel_idx];
                    break;
                }
            }

            for (size_t i = range.begin(); i != range.end(); ++i) {
                auto& entry = values[i];
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

    log(fmt::format("Finding match in {}", timer.lap()));

    result.finalize();
}
