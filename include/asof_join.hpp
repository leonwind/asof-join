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
    ASOFJoin(Prices& prices, OrderBook& order_book, Comparison comp_type, JoinType join_type):
        result(), prices(prices), order_book(order_book),
        comp_type(comp_type), join_type(join_type) {}

    virtual void join() = 0;

    ResultRelation result;

protected:
    Prices& prices;
    OrderBook& order_book;
    Comparison comp_type;
    JoinType join_type;
};

class BaselineASOFJoin : public ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;
    void join() override;
};

class SortingASOFJoin : public ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;
    void join() override;
};

class PartitioningLeftASOFJoin : public ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;
    void join() override;
};

class PartitioningRightASOFJoin : public ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;
    void join() override;
};

class PartitioningSortedMergeJoin : public ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;
    void join() override;
};

#endif //ASOF_JOIN_ASOF_JOIN_HPP
