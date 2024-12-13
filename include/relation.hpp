#ifndef ASOF_JOIN_RELATION_HPP
#define ASOF_JOIN_RELATION_HPP

#include <utility>
#include <vector>
#include <string>
#include <iostream>
#include <cstdint>

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
};

Prices load_prices(std::string_view path, char delimiter = ',', bool shuffle = false);

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
};

OrderBook load_order_book(std::string_view path, char delimiter = ',', bool shuffle = false);

struct ResultRelation : Relation {

    ResultRelation(): Relation(), simulate_pipelining(false), value_sum(0) {}
    explicit ResultRelation(bool simulate_pipelining): Relation(), simulate_pipelining(simulate_pipelining),
        value_sum(0) {}

    bool simulate_pipelining;
    uint64_t value_sum;

    struct OutputData {
        std::vector<uint64_t> prices_timestamps;
        std::vector<std::string> prices_stock_ids;
        std::vector<uint64_t> prices;
        std::vector<uint64_t> order_book_timestamps;
        std::vector<std::string> order_book_stock_ids;
        std::vector<uint64_t> amounts;
        std::vector<uint64_t> values;
    };

    tbb::enumerable_thread_specific<OutputData> thread_data;

    void insert(
        uint64_t price_timestamp,
        std::string& price_stock_id,
        uint64_t price,
        uint64_t order_book_timestamp,
        std::string& order_book_stock_id,
        uint64_t amount) {

        auto& data = thread_data.local();

        data.prices_timestamps.push_back(price_timestamp);
        data.prices_stock_ids.push_back(price_stock_id);
        data.prices.push_back(price);

        data.order_book_timestamps.push_back(order_book_timestamp);
        data.order_book_stock_ids.push_back(order_book_stock_id);
        data.amounts.push_back(amount);

        data.values.push_back(price * amount);
    }

    void finalize() {
        size_t total_size = 0;
        for (auto& data : thread_data) {
            total_size += data.values.size();
            for (auto value : data.values) {
                value_sum += value;
            }
        }
        size = total_size;
    }

    void reset() {
        thread_data.clear();
    }

    std::vector<uint64_t> collect_values() {
        std::vector<uint64_t> all_values;
        for (auto& data : thread_data) {
            all_values.insert(all_values.end(), data.values.begin(), data.values.end());
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
