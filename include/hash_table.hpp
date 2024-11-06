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
class LazyMultiMap {
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
        // Entry pointer
        Entry *ptr_;
        // Key that is being searched for
        Key key_;
    };

public:

    void insert(const Entry& val) {
        entries_.emplace_back(val);
    }

    void finalize() {
        auto num_entries = entries_.size();
        auto hash_table_size = NextPow2_32(num_entries);
        mask_ = hash_table_size - 1;
        hash_table_.resize(hash_table_size);

        // For each entry, calculate the hash and insert it at the head of the collision list.
        for (Entry& entry : entries_) {
            size_t idx = hasher(entry.key) & mask_;
            entry.next = hash_table_[idx];
            hash_table_[idx] = &entry;
        }
    }

    bool contains(Key key) {
        auto hash_table_i = hasher(key) & mask_;
        Entry *curr = hash_table_[hash_table_i];

        while (curr != nullptr && curr->key != key) {
            curr = curr->next;
        }
        return curr != nullptr;
    }


    std::pair<EqualRangeIterator, EqualRangeIterator> equal_range(Key key) {
        auto idx = hasher(key) & mask_;

        Entry* curr = hash_table_[idx];
        while (curr != nullptr && curr->key != key) {
            curr = curr->next;
        }

        EqualRangeIterator begin = EqualRangeIterator(curr, key);
        return {begin, EqualRangeIterator()};
    }

 protected:
    std::vector<Entry> entries_;
    std::vector<Entry*> hash_table_;
    std::hash<Key> hasher;
    uint32_t mask_;


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
