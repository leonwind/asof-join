#ifndef ASOF_JOIN_RELATION_HPP
#define ASOF_JOIN_RELATION_HPP

#include <utility>
#include <vector>
#include <string>
#include <iostream>
#include <cstdint>

#include "tuple_buffer.hpp"

#include "tbb/enumerable_thread_specific.h"

struct Relation {
    size_t size;
};

struct Prices : Relation {
    std::vector<uint64_t> timestamps;
    std::vector<std::string> stock_ids;
    std::vector<uint64_t> prices;

    Prices(std::vector<uint64_t>& timestamps,
           std::vector<std::string>& stock_ids,
           std::vector<uint64_t>& prices,
           size_t size):
        Relation(size),
        timestamps(std::move(timestamps)),
        stock_ids(std::move(stock_ids)),
        prices(std::move(prices)) {};

    [[nodiscard]] size_t total_size() const {
        size_t total_size = sizeof(Prices);
        total_size += timestamps.size() * sizeof(uint64_t);
        total_size += prices.size() * sizeof(uint64_t);
        total_size += stock_ids.size() * sizeof(std::string);
        for (const auto& stock_id : stock_ids) {
            total_size += stock_id.size();
        }
        return total_size;
    }
};

Prices load_prices(std::string_view path, char delimiter = ',', bool shuffle = false);
Prices generate_equal_distributed_prices(size_t num_prices, size_t price_sampling_rate, size_t num_diff_stocks);
Prices generate_uniform_prices(size_t num_prices, size_t max_timestamp, size_t num_diff_stocks);
Prices shuffle_prices(Prices& prices);
Prices select_first_n_prices(Prices& prices_og, size_t n);

struct OrderBook : Relation {
    std::vector<uint64_t> timestamps;
    std::vector<std::string> stock_ids;
    std::vector<uint64_t> amounts;

    OrderBook(std::vector<uint64_t>& timestamps,
              std::vector<std::string>& stock_ids,
              std::vector<uint64_t>& amounts,
              size_t size):
        Relation(size),
        timestamps(std::move(timestamps)),
        stock_ids(std::move(stock_ids)),
        amounts(std::move(amounts)) {};

    [[nodiscard]] size_t total_size() const {
        size_t total_size = sizeof(OrderBook);
        total_size += timestamps.size() * sizeof(uint64_t);
        total_size += amounts.size() * sizeof(uint64_t);
        total_size += stock_ids.size() * sizeof(std::string);
        for (const auto& stock_id : stock_ids) {
            total_size += stock_id.size();
        }
        return total_size;
    }
};

OrderBook load_order_book(std::string_view path, char delimiter = ',', bool shuffle = false);
OrderBook generate_uniform_orderbook(size_t num_orders, size_t max_timestamp, size_t num_diff_stocks);
OrderBook generate_uniform_orderbook(
        size_t num_orders, size_t min_timestamp, size_t max_timestamp, size_t num_diff_stocks);
OrderBook generate_zipf_uniform_orderbook(
        size_t num_orders, size_t max_timestamp, size_t num_diff_stocks, double zipf_skew);
OrderBook select_first_n_orders(OrderBook& order_book, size_t n);

struct ResultRelation : Relation {

    ResultRelation(): Relation(), value_sum(0) {}

    uint64_t value_sum;

    struct OutputData {
        size_t batch_size = 1024;

        std::vector<uint64_t> prices_timestamps;
        std::vector<std::string_view> prices_stock_ids;
        std::vector<uint64_t> prices;
        std::vector<uint64_t> order_book_timestamps;
        std::vector<std::string_view> order_book_stock_ids;
        std::vector<uint64_t> amounts;
        std::vector<uint64_t> values;
        size_t batch_idx;
        size_t size;
        size_t value_sum;

        OutputData():
            prices_timestamps(batch_size),
            prices_stock_ids(batch_size),
            prices(batch_size),
            order_book_timestamps(batch_size),
            order_book_stock_ids(batch_size),
            amounts(batch_size),
            values(batch_size),
            batch_idx(0),
            size(0),
            value_sum(0) {}
    };

    tbb::enumerable_thread_specific<OutputData> thread_data;


    inline void insert(
        uint64_t price_timestamp,
        std::string_view price_stock_id,
        uint64_t price,
        uint64_t order_book_timestamp,
        std::string_view order_book_stock_id,
        uint64_t amount) {

        auto& data = thread_data.local();
        size_t idx = data.batch_idx & (data.batch_size - 1);

        //data.prices_timestamps[idx] = price_timestamp;
        //data.prices_stock_ids[idx] = price_stock_id;
        //data.prices[idx] = price;

        data.order_book_timestamps[idx] = order_book_timestamp;
        data.order_book_stock_ids[idx] = order_book_stock_id;
        //data.amounts[idx] = amount;

        size_t value = price * amount;
        data.values[idx] = value;
        data.value_sum += value;

        ++data.size;
        ++data.batch_idx;

        //data.prices_timestamps.push_back(price_timestamp);
        //data.prices_stock_ids.push_back(price_stock_id);
        //data.prices.push_back(price);

        //data.order_book_timestamps.push_back(order_book_timestamp);
        //data.order_book_stock_ids.push_back(order_book_stock_id);
        //data.amounts.push_back(amount);

        //data.values.push_back(price * amount);
    }

    void finalize() {
        size_t total_size = 0;
        for (auto& data : thread_data) {
            total_size += data.size;
            value_sum += data.value_sum;
        }
        size = total_size;
    }

    void reset() {
        thread_data.clear();
    }

    std::vector<uint64_t> collect_values() {
        std::vector<uint64_t> all_values;
        for (auto& data : thread_data) {
            auto count = std::min(data.size, data.batch_size);
            all_values.insert(all_values.end(), data.values.begin(), data.values.begin() + count);
        }
        return all_values;
    }

    void print() {
        std::cout
            << "Price t, Price Stock, Price, "
            << "OrderBook t, OrderBook Stock, Amount, Value" << std::endl;
        //for (size_t i = 0; i < size; ++i) {
        //    std::cout
        //        << prices_timestamps[i] << ","
        //        << prices_stock_ids[i] << ","
        //        << prices[i] << ","
        //        << order_book_timestamps[i] << ","
        //        << order_book_stock_ids[i] << ","
        //        << amounts[i] << ","
        //        << values[i] << std::endl;
        //}
    }
};

#endif //ASOF_JOIN_RELATION_HPP
