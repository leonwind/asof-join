#ifndef ASOF_JOIN_RELATION_HPP
#define ASOF_JOIN_RELATION_HPP

#include <utility>
#include <vector>
#include <string>
#include <iostream>
#include <cstdint>

struct Relation {
    size_t size;
};

struct Prices : Relation {
    std::vector<uint64_t> timestamps;
    std::vector<std::string> stock_ids;
    std::vector<uint64_t> prices;

    Prices(std::vector<uint64_t> timestamps,
           std::vector<std::string> stock_ids,
           std::vector<uint64_t> prices,
           size_t size):
        Relation(size),
        timestamps(std::move(timestamps)),
        stock_ids(std::move(stock_ids)),
        prices(std::move(prices)) {};
};

Prices load_prices(std::string_view path, char delimiter = ',');

struct OrderBook : Relation {
    std::vector<uint64_t> timestamps;
    std::vector<std::string> stock_ids;
    std::vector<uint64_t> amounts;

    OrderBook(std::vector<uint64_t> timestamps,
              std::vector<std::string> stock_ids,
              std::vector<uint64_t> amounts,
              size_t size):
        Relation(size),
        timestamps(std::move(timestamps)),
        stock_ids(std::move(stock_ids)),
        amounts(std::move(amounts)) {};
};

OrderBook load_order_book(std::string_view path, char delimiter = ',');

struct ResultRelation : Relation {
    std::vector<uint64_t> prices_timestamps;
    std::vector<std::string> prices_stock_ids;
    std::vector<uint64_t> prices;
    std::vector<uint64_t> order_book_timestamps;
    std::vector<std::string> order_book_stock_ids;
    std::vector<uint64_t> amounts;
    std::vector<uint64_t> values;

    ResultRelation(Relation& left, Relation& right) : Relation() {
        size_t capacity = left.size > right.size ? left.size : right.size;

        prices_timestamps.reserve(capacity);
        prices_stock_ids.reserve(capacity);
        prices.reserve(capacity);
        order_book_timestamps.reserve(capacity);
        order_book_stock_ids.reserve(capacity);
        amounts.reserve(capacity);
        values.reserve(capacity);
    }

    void finalize() {
        size = prices_timestamps.size();
    }

    void print() {
        std::cout
            << "Price t, Price Stock, Price,"
            << "OrderBook t, OrderBook Stock, Amount, Value" << std::endl;
        for (size_t i = 0; i < size; ++i) {
            std::cout
                << prices_timestamps[i] << ","
                << prices_stock_ids[i] << ","
                << prices[i] << ","
                << order_book_timestamps[i] << ","
                << order_book_stock_ids[i] << ","
                << amounts[i] << ","
                << values[i] << std::endl;
        }
    }
};

#endif //ASOF_JOIN_RELATION_HPP
