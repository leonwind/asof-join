#ifndef ASOF_JOIN_ASOF_JOIN_H
#define ASOF_JOIN_ASOF_JOIN_H

#include <relation.h>

enum Comparison {
    LESS_THAN,
    LESS_EQUAL_THAN,
    EQUAL,
    GREATER_THAN,
    GREATER_EQUAL_THEN,
};

class ASOFJoin {

public:
    ASOFJoin(Prices prices, Orderbook order_book, Comparison comp_type):
        prices(std::move(prices)), order_book(std::move(order_book)), comp_type(comp_type) {}

    ASOFJoin(Prices& prices, Orderbook& order_book, Comparison comp_type):
        prices(prices), order_book(order_book), comp_type(comp_type) {}

    [[nodiscard]] ResultRelation join();

private:
    Prices prices;
    Orderbook order_book;
    Comparison comp_type;
};

#endif //ASOF_JOIN_ASOF_JOIN_H
