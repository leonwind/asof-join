#include "asof_join.hpp"
#include "timer.hpp"
#include "spin_lock.hpp"
#include <unordered_map>
#include <mutex>
#include "tbb/parallel_sort.h"
#include "tbb/parallel_for_each.h"


// Morsel size is 16384
#define MORSEL_SIZE (2<<14)

struct TimestampIdx {
    uint64_t timestamp;
    size_t idx;

    TimestampIdx(uint64_t timestamp, size_t idx): timestamp(timestamp), idx(idx) {}

    std::strong_ordering operator <=> (const TimestampIdx& other) const {
        return timestamp <=> other.timestamp;
    }
};

ResultRelation PartitioningSortedMergeJoin::join() {
    ResultRelation result(prices, order_book);

    Timer timer;
    timer.start();

    std::unordered_map<std::string_view, std::vector<TimestampIdx>> order_book_index;
    for (size_t i = 0; i < order_book.size; ++i) {
        order_book_index[order_book.stock_ids[i]].emplace_back(
            /* timestamp= */ order_book.timestamps[i],
            /* idx= */ i);
    }

    std::unordered_map<std::string_view, std::vector<TimestampIdx>> prices_index;
    for (size_t i = 0; i < prices.size; ++i) {
        prices_index[prices.stock_ids[i]].emplace_back(
            /* timestamp = */ prices.timestamps[i],
            /* idx= */ i);
    }

    std::cout << "Partitioning in " << timer.lap() << std::endl;

    tbb::parallel_for_each(prices_index.begin(), prices_index.end(),
            [&](auto& iter) {
        tbb::parallel_sort(iter.second.begin(), iter.second.end());
    });

    tbb::parallel_for_each(order_book_index.begin(), order_book_index.end(),
            [&](auto& iter) {
        tbb::parallel_sort(iter.second.begin(), iter.second.end());
    });

    std::cout << "Sorting in " << timer.lap() << std::endl;

    std::mutex result_lock;
    tbb::parallel_for_each(order_book_index.begin(), order_book_index.end(),
            [&](auto& iter) {
        const std::vector<TimestampIdx>& orders_bin = order_book_index[iter.first];
        const std::vector<TimestampIdx>& prices_bin = prices_index[iter.first];

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

    std::cout << "Merge join in " << timer.lap() << std::endl;

    result.finalize();
    return result;
}
