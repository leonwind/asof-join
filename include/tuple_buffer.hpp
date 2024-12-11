#ifndef ASOF_JOIN_TUPLE_BUFFER_HPP
#define ASOF_JOIN_TUPLE_BUFFER_HPP

#include <iostream>
#include <memory>
#include <vector>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <bits/stdc++.h>

namespace {
    const uint64_t PAGE_SIZE_4KB = 4096;

    inline constexpr uint64_t next_pow2_64(uint64_t v) {
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v |= v >> 32;
        v++;
        return v;
    }
} // namespace;

class Buffer {
   size_t buffer_size = 0;
   size_t bytes_used = 0;
   void *chunk = nullptr;

public:
    static std::unique_ptr<Buffer> create(size_t buffer_size) {
        auto buffer = std::make_unique<Buffer>();
        buffer->chunk = mmap(nullptr, buffer_size, PROT_READ | PROT_WRITE,
                     MAP_SHARED | MAP_ANONYMOUS, -1, 0);
        buffer->buffer_size = buffer_size;
        buffer->bytes_used = 0;
        return buffer;
    }

    ~Buffer() { munmap(chunk, buffer_size); }

    char* alloc_tuple(size_t tuple_size) {
        char* ptr = reinterpret_cast<char*>(chunk) + bytes_used;
        bytes_used += tuple_size;
        return ptr;
    }

    [[nodiscard]] inline bool has_space(size_t tuple_size) const {
        return tuple_size + bytes_used <= buffer_size;
    }

    [[nodiscard]] char* data() {
        return reinterpret_cast<char*>(chunk);
    }

    [[nodiscard]] const char* data() const {
        return reinterpret_cast<const char*>(chunk);
    }

    [[nodiscard]] inline size_t get_bytes_used() const { return bytes_used; }
};

template<typename T>
class TupleBuffer {
    /// Fixed sized tuples
    static constexpr uint64_t TUPLE_SIZE = sizeof(T);
    /// Minimum 4KB and then round up to next power of 2.
    static constexpr uint64_t BUFFER_CAPACITY = next_pow2_64(PAGE_SIZE_4KB / TUPLE_SIZE);
    static constexpr uint64_t BUFFER_CAPACITY_MASK = BUFFER_CAPACITY - 1;
    static constexpr uint64_t BUFFER_SIZE = TUPLE_SIZE * BUFFER_CAPACITY;

    std::unique_ptr<Buffer> curr;
    std::vector<std::unique_ptr<Buffer>> old_buffers;
    size_t num_tuples;

public:
    class Iterator;

    TupleBuffer(): curr(Buffer::create(BUFFER_SIZE)), old_buffers(), num_tuples(0) {}
    ~TupleBuffer() = default;

    char* next_slot() {
        ++num_tuples;
        if (!curr->has_space(TUPLE_SIZE)) [[unlikely]] {
            old_buffers.push_back(std::move(curr));
            curr = Buffer::create(BUFFER_SIZE);
        }
        return curr->alloc_tuple(TUPLE_SIZE);
    }

    inline void store_tuple(T tuple) {
        auto* memory = reinterpret_cast<T*>(next_slot());
        new(memory) T(std::move(tuple));
    }

    [[nodiscard]] inline size_t size() const {
        return num_tuples;
    }

    [[nodiscard]] inline T& operator[](size_t i) const {
        size_t buffer_idx = i / BUFFER_CAPACITY;
        size_t tuple_idx = i & BUFFER_CAPACITY_MASK;
        return get_tuple(buffer_idx, tuple_idx);
    }

    Iterator begin() const {
        return Iterator(this, /* buffer_idx= */ 0, /* tuple_idx= */ 0);
    }

    Iterator end() const {
        return Iterator(this,
            /* buffer_idx= */ old_buffers.size(),
            /* tuple_idx= */ curr->get_bytes_used() / TUPLE_SIZE);
    }

private:
    T& get_tuple(size_t buffer_idx, size_t tuple_idx) const {
        auto *data = buffer_idx == old_buffers.size()
            ? curr->data()
            : old_buffers[buffer_idx]->data();

        return *reinterpret_cast<T*>(data + tuple_idx * TUPLE_SIZE);
    }
};

template<typename T>
class TupleBuffer<T>::Iterator {
public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = T;
    using difference_type = std::ptrdiff_t;

    Iterator(const TupleBuffer<T>* buffer, size_t buffer_idx, size_t tuple_idx):
            buffer_(buffer), buffer_idx_(buffer_idx), tuple_idx_(tuple_idx) {}

    T& operator*() const {
        return buffer_->get_tuple(buffer_idx_, tuple_idx_);
    }

    T* operator->() const {
        return &(**this);
    }

    /// Pre-increment
    Iterator& operator++() {
        ++tuple_idx_;
        advance_to_next_tuple();
        return *this;
    }

    /// Post-increment
    Iterator operator++(int) {
        Iterator tmp = *this;
        ++(*this);
        return tmp;
    }

    bool operator==(const Iterator& other) const {
        return buffer_ == other.buffer_ &&
               buffer_idx_ == other.buffer_idx_ &&
               tuple_idx_ == other.tuple_idx_;
    }

    bool operator!=(const Iterator& other) const {
        /// Simplifying this expression makes it recursive.
        return !(*this == other);
    }

private:
    const TupleBuffer<T>* buffer_;
    size_t buffer_idx_;
    size_t tuple_idx_;

    void advance_to_next_tuple() {
        if (buffer_idx_ < buffer_->old_buffers.size()) {
            if (tuple_idx_ >= buffer_->BUFFER_CAPACITY) {
                ++buffer_idx_;
                tuple_idx_ = 0;
            }
        } else if (buffer_idx_ == buffer_->old_buffers.size()) {
            if (tuple_idx_ >= (buffer_->curr->get_bytes_used() / TUPLE_SIZE)) {
                buffer_idx_ = buffer_->old_buffers.size();
                tuple_idx_ = buffer_->curr->get_bytes_used() / TUPLE_SIZE;
            }
        }
        //if (tuple_idx_ >= buffer_->BUFFER_CAPACITY) {
        //    ++buffer_idx_;
        //    tuple_idx_ = 0;
        //}
    }
};

#endif // ASOF_JOIN_TUPLE_BUFFER_HPP
