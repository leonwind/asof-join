#ifndef ASOF_JOIN_ASOF_JOIN_HPP
#define ASOF_JOIN_ASOF_JOIN_HPP

#include <relation.hpp>
#include <perfevent.hpp>
#include <iostream>
#include "spin_lock.hpp"
#include <atomic>


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
             Comparison comp_type, JoinType join_type):
        prices(prices), order_book(order_book),
        comp_type(comp_type), join_type(join_type) {}

    virtual void join() = 0;

    ResultRelation result;

    [[nodiscard]] virtual std::string_view get_strategy_name() const = 0;

    struct JoinEntry {
        virtual ~JoinEntry() = default;

        [[nodiscard]] [[gnu::always_inline]] virtual uint64_t get_key() const = 0;
        
        virtual std::strong_ordering operator<=>(const JoinEntry &other) const {
            return get_key() <=> other.get_key();
        }
    };

    struct RightEntry;
    struct LeftEntry;
    struct LeftEntryCopy;

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

    [[nodiscard]] std::string_view get_strategy_name() const override {
        return "BASELINE";
    }
};

class SortingASOFJoin : public ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;
    void join() override;

    [[nodiscard]] std::string_view get_strategy_name() const override {
        return "SORTING NO PARTITIONING";
    }
};

class PartitioningRightASOFJoin : public ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;
    void join() override;

    [[nodiscard]] std::string_view get_strategy_name() const override {
        return "PARTITION RIGHT";
    }
};

class PartitioningLeftASOFJoin : public ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;
    void join() override;

    [[nodiscard]] std::string_view get_strategy_name() const override {
        return "PARTITION LEFT";
    }
};

class PartitioningSortedMergeJoin : public ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;
    void join() override;

    [[nodiscard]] std::string_view get_strategy_name() const override {
        return "PARTITION SORTED MERGE";
    }
};

class PartitioningRightBTreeASOFJoin : public ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;
    void join() override;

    [[nodiscard]] std::string_view get_strategy_name() const override {
        return "PARTITION RIGHT BTREE";
    }
};

class PartitioningLeftBTreeASOFJoin : public ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;
    void join() override;

    [[nodiscard]] std::string_view get_strategy_name() const override {
        return "PARTITION LEFT BTREE";
    }
};

class PartitioningBothSortLeftASOFJoin : public ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;
    void join() override;

    [[nodiscard]] std::string_view get_strategy_name() const override {
        return "PARTITION BOTH + SORT LEFT";
    }
};

class PartitioningRightFilterMinASOFJoin : public ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;
    void join() override;

    [[nodiscard]] std::string_view get_strategy_name() const override {
        return "PARTITION RIGHT + FILTER MIN";
    }
};

class PartitioningLeftFilterMinASOFJoin : public ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;
    void join() override;

    [[nodiscard]] std::string_view get_strategy_name() const override {
        return "PARTITION LEFT + FILTER MIN";
    }
};

class PartitioningLeftCopyLeftASOFJoin : public ASOFJoin {
public:
    using ASOFJoin::ASOFJoin;
    void join() override;

    [[nodiscard]] std::string_view get_strategy_name() const override {
        return "PARTITION + COPY LEFT";
    }
};

struct ASOFJoin::RightEntry : JoinEntry {
    uint64_t timestamp;
    size_t idx;
    RightEntry(): timestamp(-1), idx(-1) {}
    RightEntry(uint64_t timestamp, size_t idx) : timestamp(timestamp), idx(idx) {}

    [[nodiscard]] [[gnu::always_inline]] uint64_t get_key() const override {
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

    //struct DiffPrice {
    //    uint64_t diff;
    //    uint64_t price_idx;
    //};
    //std::atomic<DiffPrice> diff_price;

    LeftEntry(): timestamp(-1), order_idx(-1),
                 price_idx(-1), diff(-1), matched(false), lock() {}
                 //price_idx(-1), diff(-1), matched(false), lock(), diff_price(DiffPrice(-1, -1)) {}

    LeftEntry(uint64_t timestamp, size_t order_idx): timestamp(timestamp), order_idx(order_idx),
                                                     price_idx(0), diff(UINT64_MAX), matched(false), lock() {}
                                                     //, diff_price(DiffPrice(UINT64_MAX, 0)) {}

    LeftEntry(const LeftEntry &other) : timestamp(other.timestamp), order_idx(other.order_idx),
                                        price_idx(other.price_idx), diff(other.diff), matched(other.matched),
                                        lock() {
        //diff_price.store(other.diff_price);
    }

    LeftEntry(LeftEntry &&other) noexcept: timestamp(other.timestamp), order_idx(other.order_idx),
                                           price_idx(other.price_idx), diff(other.diff), matched(other.matched),
                                           lock() {
        //diff_price.store(other.diff_price);
    }

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

    void inline lock_compare_swap_diffs(uint64_t new_diff, uint64_t new_price_idx) {
        /// Use (spin) lock while comparing and exchanging the diff and price idx.
        lock.lock();
        if (new_diff < diff) {
            diff = new_diff;
            price_idx = new_price_idx;
        }
        lock.unlock();
        matched = true;
    }

    //void inline atomic_compare_swap_diffs(uint64_t new_diff, uint64_t new_price_idx) {
    //    /// Use atomic CAS statement to compare and exchange the diff and price idx.
    //    matched = true;
    //    DiffPrice desired{new_diff, new_price_idx};

    //    DiffPrice old_val = diff_price.load(std::memory_order_relaxed);
    //    while (new_diff < old_val.diff) {
    //        if (diff_price.compare_exchange_weak(
    //            /* expected= */ old_val,
    //            /* desired= */ desired,
    //            /* success= */ std::memory_order_acquire,
    //            /* failure= */ std::memory_order_relaxed)) { break; }
    //    }
    //}

    [[nodiscard]] [[gnu::always_inline]] uint64_t get_key() const override {
        return timestamp;
    }
};

struct ASOFJoin::LeftEntryCopy : JoinEntry {
    uint64_t timestamp;
    size_t order_idx;
    size_t price_idx;
    uint64_t diff;
    bool matched;

    LeftEntryCopy(): timestamp(-1), order_idx(-1),
                 price_idx(-1), diff(-1), matched(false) {}

    LeftEntryCopy(uint64_t timestamp, size_t order_idx): timestamp(timestamp), order_idx(order_idx),
                                                     price_idx(0), diff(UINT64_MAX), matched(false) {}

    LeftEntryCopy(const LeftEntryCopy &other) : timestamp(other.timestamp), order_idx(other.order_idx),
                                        price_idx(other.price_idx), diff(other.diff), matched(other.matched) {}

    LeftEntryCopy(LeftEntryCopy &&other) noexcept: timestamp(other.timestamp), order_idx(other.order_idx),
                                           price_idx(other.price_idx), diff(other.diff), matched(other.matched) {}

    LeftEntryCopy &operator=(const LeftEntryCopy &other) {
        if (this != &other) {
            timestamp = other.timestamp;
            order_idx = other.order_idx;
            price_idx = other.price_idx;
            diff = other.diff;
            matched = other.matched;
        }
        return *this;
    }

    LeftEntryCopy &operator=(LeftEntryCopy &&other) noexcept {
        if (this != &other) {
            timestamp = other.timestamp;
            order_idx = other.order_idx;
            price_idx = other.price_idx;
            diff = other.diff;
            matched = other.matched;
        }
        return *this;
    }

    [[nodiscard]] [[gnu::always_inline]] uint64_t get_key() const override {
        return timestamp;
    }
};

#endif //ASOF_JOIN_ASOF_JOIN_HPP
