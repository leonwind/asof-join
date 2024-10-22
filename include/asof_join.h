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
    ASOFJoin(Prices prices, OrderBook order_book, Comparison comp_type):
        prices(std::move(prices)), order_book(std::move(order_book)), comp_type(comp_type) {}

    [[nodiscard]] virtual ResultRelation join() = 0;

protected:
    Prices prices;
    OrderBook order_book;
    Comparison comp_type;
};

class BaselineASOFJoin : ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;
    [[nodiscard]] ResultRelation join() override;
};

class SortingASOFJoin : ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;
    [[nodiscard]] ResultRelation join() override;
};

class PartitioningASOFJoin : ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;
    [[nodiscard]] ResultRelation join() override;
};

#endif //ASOF_JOIN_ASOF_JOIN_H
