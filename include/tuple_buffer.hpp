#ifndef ASOF_JOIN_TUPLE_BUFFER_HPP
#define ASOF_JOIN_TUPLE_BUFFER_HPP

#include <iostream>
#include <memory>
#include <vector>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <bits/stdc++.h>

class Buffer {
   /// Buffer size is 4KiB to reduce TLB misses.
   static constexpr size_t BUFFER_SIZE = 4096;

   void *chunk = nullptr;
   size_t bytes_used = 0;

public:
    static std::unique_ptr<Buffer> create() {
        auto buffer = std::make_unique<Buffer>();
        buffer->chunk = mmap(nullptr, BUFFER_SIZE, PROT_READ | PROT_WRITE,
                     MAP_SHARED | MAP_ANONYMOUS, -1, 0);
        return buffer;
    }

    ~Buffer() { munmap(chunk, BUFFER_SIZE); }

    char* alloc_tuple(size_t size) {
        char* ptr = reinterpret_cast<char*>(chunk) + bytes_used;
        bytes_used += size;
        return ptr;
    }

    [[nodiscard]] inline bool has_space(size_t tuple_size) const {
        return tuple_size + bytes_used < BUFFER_SIZE;
    }

    [[nodiscard]] char* data() {
        return reinterpret_cast<char*>(chunk);
    }

    [[nodiscard]] const char* data() const {
        return reinterpret_cast<const char*>(chunk);
    }

    [[nodiscard]] inline size_t get_bytes_used() const { return bytes_used; }

    [[nodiscard]] static inline size_t capacity(size_t tuple_size) { return BUFFER_SIZE / tuple_size; }
};

template<typename T>
class TupleBuffer {
    /// Fixed sized tuples
    const size_t tuple_size;
    std::unique_ptr<Buffer> curr;
    std::vector<std::unique_ptr<Buffer>> old_buffers;
    size_t num_tuples;
    size_t buffer_capacity;

public:
    /// Forward declaration
    class Iterator;

    TupleBuffer(): tuple_size(sizeof(T)), curr(Buffer::create()), old_buffers(),
        num_tuples(0), buffer_capacity(Buffer::capacity(sizeof(T))) {}

    ~TupleBuffer() = default;

    char* next_slot() {
        ++num_tuples;
        if (!curr->has_space(tuple_size)) [[unlikely]] {
            old_buffers.push_back(std::move(curr));
            curr = Buffer::create();
        }
        return curr->alloc_tuple(tuple_size);
    }

    inline void store_tuple(T tuple) {
        auto* memory = reinterpret_cast<T*>(next_slot());
        new(memory) T(std::move(tuple));
    }

    [[nodiscard]] inline size_t size() const {
        return num_tuples;
    }

    [[nodiscard]] inline T& operator[](size_t i) const {
        /// TODO: Make buffer_capacity power of 2 to use bitwise AND instead of modulo operator.
        size_t buffer_idx = i / buffer_capacity;
        size_t tuple_idx = i % buffer_capacity;
        return get_tuple(buffer_idx, tuple_idx);
    }

    Iterator begin() const {
        return Iterator(this, /* buffer_idx= */ 0, /* tuple_idx= */ 0);
    }

    Iterator end() const {
        return Iterator(this,
            /* buffer_idx= */ old_buffers.size(),
            /* tuple_idx= */ curr->get_bytes_used() / tuple_size);
    }

private:
    T& get_tuple(size_t buffer_idx, size_t tuple_idx) const {
        auto *data = buffer_idx == old_buffers.size()
                ? curr->data()
                : old_buffers[buffer_idx]->data();

        return *reinterpret_cast<T*>(data + tuple_size * tuple_idx);
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

    bool operator==(const Iterator& other) {
        return buffer_ == other.buffer_ &&
               buffer_idx_ == other.buffer_idx_ &&
               tuple_idx_ == other.tuple_idx_;
    }

    bool operator!=(const Iterator& other) {
        /// Simplifying this expression makes it recursive.
        return !(*this == other);
    }

private:
    const TupleBuffer<T>* buffer_;
    size_t buffer_idx_;
    size_t tuple_idx_;

    void advance_to_next_tuple() {
        if (tuple_idx_ >= buffer_->buffer_capacity) {
            ++buffer_idx_;
            tuple_idx_ = 0;
        }
    }
};

#endif // ASOF_JOIN_TUPLE_BUFFER_HPP
