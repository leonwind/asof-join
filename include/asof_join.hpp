#ifndef ASOF_JOIN_ASOF_JOIN_HPP
#define ASOF_JOIN_ASOF_JOIN_HPP

#include <relation.hpp>
#include <perfevent.hpp>
#include <iostream>
#include "spin_lock.hpp"

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
    ASOFJoin(Prices& prices, OrderBook& order_book,
             Comparison comp_type, JoinType join_type, bool simulate_pipelining = false):
        result(simulate_pipelining), prices(prices), order_book(order_book),
        comp_type(comp_type), join_type(join_type) {}

    virtual void join() = 0;

    ResultRelation result;

    struct JoinEntry {
        virtual ~JoinEntry() = default;

        [[nodiscard]] inline virtual uint64_t get_key() const = 0;
        
        virtual std::strong_ordering operator<=>(const JoinEntry &other) const {
            return get_key() <=> other.get_key();
        }
    };

    struct RightEntry;
    struct LeftEntry;

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

class PartitioningRightASOFJoin : public ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;
    void join() override;

private:
    static RightEntry* binary_search_closest_match_less_than(
            std::vector<RightEntry>& data, uint64_t target);
};

class PartitioningLeftASOFJoin : public ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;
    void join() override;

private:
    static LeftEntry* binary_search_closest_match_greater_than(
            std::vector<LeftEntry>&data, uint64_t target);
};

class PartitioningSortedMergeJoin : public ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;
    void join() override;
};

class PartitioningRightBTreeASOFJoin : public ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;
    void join() override;
};

class PartitioningLeftBTreeASOFJoin : public ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;
    void join() override;
};

class PartitioningBothSortLeftASOFJoin : public ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;
    void join() override;

private:
    static LeftEntry* binary_search_closest_match_greater_than(
            std::vector<LeftEntry>& data, uint64_t target);
};

struct ASOFJoin::RightEntry : JoinEntry {
    uint64_t timestamp;
    size_t idx;
    RightEntry(): timestamp(-1), idx(-1) {}
    RightEntry(uint64_t timestamp, size_t idx) : timestamp(timestamp), idx(idx) {}

    [[nodiscard]] inline uint64_t get_key() const override {
        return timestamp;
    }
};

struct ASOFJoin::LeftEntry : JoinEntry {
    uint64_t timestamp;
    size_t order_idx;
    size_t price_idx;
    uint64_t diff;
    bool matched;
    SpinLock lock;

    LeftEntry(): timestamp(-1), order_idx(-1),
                 price_idx(-1), diff(-1), matched(false), lock() {}

    LeftEntry(uint64_t timestamp, size_t order_idx): timestamp(timestamp), order_idx(order_idx),
                                                     price_idx(0), diff(UINT64_MAX), matched(false), lock() {}

    LeftEntry(const LeftEntry &other) : timestamp(other.timestamp), order_idx(other.order_idx),
                                        price_idx(other.price_idx), diff(other.diff), matched(other.matched), lock() {}

    LeftEntry(LeftEntry &&other) noexcept: timestamp(other.timestamp), order_idx(other.order_idx),
                                           price_idx(other.price_idx), diff(other.diff), matched(other.matched), lock() {}

    LeftEntry &operator=(const LeftEntry &other) {
        if (this != &other) {
            timestamp = other.timestamp;
            order_idx = other.order_idx;
            price_idx = other.price_idx;
            diff = other.diff;
            matched = other.matched;
        }
        return *this;
    }

    LeftEntry &operator=(LeftEntry &&other) noexcept {
        if (this != &other) {
            timestamp = other.timestamp;
            order_idx = other.order_idx;
            price_idx = other.price_idx;
            diff = other.diff;
            matched = other.matched;
        }
        return *this;
    }

    [[nodiscard]] inline uint64_t get_key() const override {
        return timestamp;
    }
};

#endif //ASOF_JOIN_ASOF_JOIN_HPP
