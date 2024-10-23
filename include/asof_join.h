#ifndef ASOF_JOIN_ASOF_JOIN_H
#define ASOF_JOIN_ASOF_JOIN_H

#include <relation.h>
#include <timer.h>
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
    [[nodiscard]] ResultRelation join() override;
};

class SortingASOFJoin : public ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;
    [[nodiscard]] ResultRelation join() override;
};

class PartitioningASOFJoin : public ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;
    [[nodiscard]] ResultRelation join() override;
};

inline void run_join(ASOFJoin& asof_op, std::string_view strategy) {
    Timer timer = Timer::start();
    auto result = asof_op.join();
    auto duration = timer.end();

    uint64_t total_sum = 0;
    for (auto value : result.values) { total_sum += value; }

    std::cout << "### ASOF JOIN TOTAL VALUE SUM: " << total_sum << std::endl;
    std::cout << "### FINISHED ASOF JOIN WITH " << strategy
        << " IN " << duration << "[us] ###" << std::endl;

}

#endif //ASOF_JOIN_ASOF_JOIN_H
