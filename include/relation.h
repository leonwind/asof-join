#ifndef ASOF_JOIN_RELATION_H
#define ASOF_JOIN_RELATION_H

#include <vector>
#include <string>
#include <cstdint>

struct Prices {
    std::vector<uint64_t> timestamps;
    std::vector<std::string> stock_ids;
    std::vector<uint64_t> prices;
    size_t size;
};

Prices load_prices(std::string_view path, char delimiter = ',');

struct Orderbook {
    std::vector<uint64_t> timestamps;
    std::vector<std::string> stock_ids;
    std::vector<uint64_t> amounts;
    size_t size;
};

Orderbook load_order_book(std::string_view path, char delimiter = ',');

struct ResultRelation {
    std::vector<uint64_t> prices_timestamps;
    std::vector<std::string> prices_stock_ids;
    std::vector<uint64_t> prices;
    std::vector<uint64_t> order_book_timestamps;
    std::vector<std::string> order_book_stock_ids;
    std::vector<uint64_t> amount;
    std::vector<uint64_t> values;
    size_t size;
};

#endif //ASOF_JOIN_RELATION_H
