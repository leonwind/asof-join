#ifndef ASOF_JOIN_ASOF_JOIN_HPP
#define ASOF_JOIN_ASOF_JOIN_HPP

#include <relation.hpp>
#include <perfevent.hpp>
#include <iostream>

enum Comparison {
    LESS_THAN,
    LESS_EQUAL_THAN,
    EQUAL,
    GREATER_THAN,
    GREATER_EQUAL_THEN,
};

enum JoinType {
    INNER,
    LEFT,
    RIGHT
};

class ASOFJoin {
public:
    const std::string STRATEGY_NAME;

    ASOFJoin(Prices prices, OrderBook order_book, Comparison comp_type, JoinType join_type):
        prices(std::move(prices)), order_book(std::move(order_book)),
        comp_type(comp_type), join_type(join_type) {}

    [[nodiscard]] virtual ResultRelation join() = 0;

protected:
    Prices prices;
    OrderBook order_book;
    Comparison comp_type;
    JoinType join_type;
};

class BaselineASOFJoin : public ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;

    const std::string STRATEGY_NAME = "nested loop";

    [[nodiscard]] ResultRelation join() override;
};

class SortingASOFJoin : public ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;

    const std::string STRATEGY_NAME = "sorted merge join";

    [[nodiscard]] ResultRelation join() override;
};

class PartitioningLeftASOFJoin : public ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;

    const std::string STRATEGY_NAME = "partition left side + binary search";

    [[nodiscard]] ResultRelation join() override;
};

class PartitioningRightASOFJoin : public ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;

    const std::string STRATEGY_NAME = "partition right side + binary search";

    [[nodiscard]] ResultRelation join() override;
};

#endif //ASOF_JOIN_ASOF_JOIN_HPP
