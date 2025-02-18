#ifndef ASOF_JOIN_HASH_TABLE_HPP
#define ASOF_JOIN_HASH_TABLE_HPP


#include <cstdint>
#include <algorithm>
#include <utility>
#include <vector>
#include <iterator>
#include <limits>
#include <string_view>

template <typename Value>
class MultiMap {
    using Key = std::string_view;

protected:
    struct Entry {
        Entry *next;
        Key key;
        Value value;

        Entry(Key key, Value value): next(nullptr), key(key), value(value) {}
    };

    class EqualRangeIterator: public std::iterator<std::forward_iterator_tag, Entry> {
     public:
        EqualRangeIterator(): ptr_(nullptr), key_() {}

        explicit EqualRangeIterator(Entry* ptr, Key key): ptr_(ptr), key_(key) {}

        ~EqualRangeIterator() = default;

        // Postfix increment
        EqualRangeIterator operator++(int) {
            Entry* tmp_ptr = ptr_;
            do {
                tmp_ptr = tmp_ptr->next;
            } while (tmp_ptr != nullptr && key_ != tmp_ptr->key);

            return EqualRangeIterator(tmp_ptr, key_);
        }

        // Prefix increment
        EqualRangeIterator& operator++() {
            do {
                ptr_ = ptr_->next;
            } while (ptr_ != nullptr && key_ != ptr_->key);
            return *this;
        }

        Entry& operator*() { return *ptr_; }

        Entry* operator->() { return ptr_; }

        bool operator==(const EqualRangeIterator& other) const { return ptr_ == other.ptr_; }

        bool operator!=(const EqualRangeIterator& other) const { return ptr_ != other.ptr_; }

     protected:
        Entry *ptr_;
        Key key_;
    };

public:

    inline uint32_t NextPow2_32(uint32_t v) {
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v++;
        return v;
    }
};
#endif //ASOF_JOIN_HASH_TABLE_HPP
