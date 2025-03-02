#ifndef ASOF_JOIN_TUPLE_BUFFER_HPP
#define ASOF_JOIN_TUPLE_BUFFER_HPP

#include "util.hpp"
#include <iostream>
#include <memory>
#include <vector>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <bits/stdc++.h>

#include "tbb/parallel_for.h"
#include "tbb/parallel_for_each.h"

namespace {
    const uint64_t PAGE_SIZE_64KB = 65536;
} // namespace;

class Buffer {
    void *chunk = nullptr;
    size_t buffer_size = 0;
    size_t bytes_used = 0;
    size_t tuple_size = 0;

public:
    Buffer() = default;
    /// Delete copy constructor and assignment
    /// since we don't have information about T's copy constructor.
    Buffer(const Buffer& other) = delete;
    Buffer& operator=(const Buffer& other) = delete;

    Buffer(Buffer&& other) noexcept: chunk(other.chunk),
            buffer_size(other.buffer_size),
            bytes_used(other.bytes_used) {
        other.chunk = nullptr;
        other.buffer_size = 0;
        other.bytes_used = 0;
    }

    ~Buffer() {
        if (chunk) {
            munmap(chunk, buffer_size);
        }
    }

    static std::unique_ptr<Buffer> create(size_t buffer_size) {
        auto buffer = std::make_unique<Buffer>();
        buffer->chunk = allocate_memory(buffer_size);
        buffer->buffer_size = buffer_size;
        buffer->bytes_used = 0;
        return buffer;
    }

    template<typename T>
    static void copy_buffer_data(const Buffer& src, Buffer& dest) {
        size_t count = src.get_bytes_used() / sizeof(T);

        const T* src_ptr = reinterpret_cast<const T*>(src.data());
        T* dest_ptr = reinterpret_cast<T*>(dest.data());

        /// Construct each object in-place by using T's copy constructor.
        for (size_t i = 0; i < count; ++i) {
            new (&dest_ptr[i]) T(src_ptr[i]);
        }

        dest.bytes_used = src.bytes_used;
    }

    char* alloc_tuple(size_t tuple_size) {
        char* ptr = reinterpret_cast<char*>(chunk) + bytes_used;
        bytes_used += tuple_size;
        return ptr;
    }

    [[nodiscard]] inline bool has_space(size_t tuple_size) const {
        return tuple_size + bytes_used <= buffer_size;
    }

    [[nodiscard]] inline char* data() {
        return reinterpret_cast<char*>(chunk);
    }

    [[nodiscard]] inline const char* data() const {
        return reinterpret_cast<const char*>(chunk);
    }

    [[nodiscard]] inline size_t get_bytes_used() const { return bytes_used; }

    [[nodiscard]] inline size_t capacity() const { return buffer_size; }

private:
    [[nodiscard]] inline static void* allocate_memory(size_t memory_size) {
        void* ptr = mmap(
            /* addr= */ nullptr,
            /* len= */ memory_size,
            /* prot= */ PROT_READ | PROT_WRITE,
            /* flags= */ MAP_PRIVATE | MAP_ANONYMOUS,
            /* fd= */ -1,
            /* offset= */ 0);
        if (ptr == MAP_FAILED) {
            perror("MMAP failed");
            std::exit(1);
        }
        return ptr;
    }
};

template<typename T>
class TupleBuffer {
    /// Fixed sized tuples
    static constexpr uint64_t TUPLE_SIZE = sizeof(T);
    /// Minimum 4KB and then round up to next power of 2.
    static constexpr uint64_t BUFFER_CAPACITY = next_pow2_64(PAGE_SIZE_64KB / TUPLE_SIZE);
    static constexpr uint64_t BUFFER_CAPACITY_MASK = BUFFER_CAPACITY - 1;
    static constexpr uint64_t BUFFER_SIZE = TUPLE_SIZE * BUFFER_CAPACITY;

    std::unique_ptr<Buffer> curr;
    std::vector<std::unique_ptr<Buffer>> old_buffers;
    size_t num_tuples;
    size_t next_buffer_size;

public:
    class Iterator;

    TupleBuffer(): curr(Buffer::create(BUFFER_SIZE)), old_buffers(), num_tuples(0),
        next_buffer_size(BUFFER_SIZE * 2) {}

    /// Copy constructor must exist for [[tbb::enumerable_thread_specific]]
    TupleBuffer(const TupleBuffer& other): num_tuples(other.num_tuples) {
        old_buffers.reserve(other.old_buffers.size());

        next_buffer_size = BUFFER_SIZE;
        for (auto& buff : other.old_buffers) {
            auto new_buffer = Buffer::create(next_buffer_size);
            next_buffer_size *= 2;
            Buffer::copy_buffer_data<T>(*buff, *new_buffer);
            old_buffers.push_back(std::move(new_buffer));
        }

        curr = Buffer::create(next_buffer_size);
        Buffer::copy_buffer_data<T>(*other.curr, *curr);
        next_buffer_size *= 2;
    }

    /// Delete copy-assignment
    TupleBuffer& operator=(const TupleBuffer& other) = delete;

    TupleBuffer(TupleBuffer&& other) noexcept: curr(std::move(other.curr)),
          old_buffers(std::move(other.old_buffers)),
          num_tuples(other.num_tuples),
          next_buffer_size(other.next_buffer_size) { other.num_tuples = 0; }

    ~TupleBuffer() {
        curr = nullptr;
        tbb::parallel_for_each(old_buffers.begin(), old_buffers.end(),
           [&](auto& buff) { buff = nullptr; });
    }

    void clear() {
        curr = Buffer::create(BUFFER_SIZE);
        old_buffers.clear();
        num_tuples = 0;
    }

    inline char* next_slot() {
        ++num_tuples;
        if (!curr->has_space(TUPLE_SIZE)) {
            old_buffers.push_back(std::move(curr));
            curr = Buffer::create(next_buffer_size);
            next_buffer_size *= 2;
        }
        return curr->alloc_tuple(TUPLE_SIZE);
    }

    inline void store_tuple(T tuple) {
        auto* memory = reinterpret_cast<T*>(next_slot());
        new(memory) T(std::move(tuple));
    }

    template <typename... Args>
    inline void emplace_back(Args&&... args) {
        auto* memory = reinterpret_cast<T*>(next_slot());
        new(memory) T(std::forward<Args>(args)...);
    }

    [[nodiscard]] inline size_t size() const {
        return num_tuples;
    }

    [[nodiscard]] inline T& operator[](size_t i) const {
        size_t buffer_idx = i / BUFFER_CAPACITY;
        size_t tuple_idx = i & BUFFER_CAPACITY_MASK;
        return get_tuple(buffer_idx, tuple_idx);
    }

    std::vector<T> copy_tuples() {
        std::vector<T> all_tuples(num_tuples);

        /// Copy all old buffer pages in parallel

        /// TODO: FIX ME
        /// Currently the offset [[n]] is wrong due to the doubling of the buffer sizes.
        tbb::blocked_range<size_t> range(0, old_buffers.size());
        tbb::parallel_for(range, [&](tbb::blocked_range<size_t>& range) {
            for (size_t i = range.begin(); i < range.end(); ++i) {
                const auto* buff = old_buffers[i].get();
                std::copy_n(
                    /* src= */ reinterpret_cast<const T*>(buff->data()),
                    /* n= */ buff->capacity() / TUPLE_SIZE,
                    /* dest= */ all_tuples.data() + i * BUFFER_CAPACITY);
            }
        });

        /// Copy curr buffer page.
        std::copy_n(
            /* src= */ reinterpret_cast<const T*>(curr->data()),
            /* n= */ curr->get_bytes_used() / TUPLE_SIZE,
            /* dest= */ all_tuples.data() + old_buffers.size() * BUFFER_CAPACITY);
        return std::move(all_tuples);
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
        advance();
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

    void advance() {
        if (buffer_idx_ < buffer_->old_buffers.size()) {
            if (tuple_idx_ >= buffer_->old_buffers[buffer_idx_]->capacity() / TUPLE_SIZE) {
                ++buffer_idx_;
                tuple_idx_ = 0;
            }
        } else if (buffer_idx_ == buffer_->old_buffers.size()) {
            if (tuple_idx_ >= (buffer_->curr->get_bytes_used() / TUPLE_SIZE)) {
                buffer_idx_ = buffer_->old_buffers.size();
                tuple_idx_ = buffer_->curr->get_bytes_used() / TUPLE_SIZE;
            }
        }
    }
};

#endif // ASOF_JOIN_TUPLE_BUFFER_HPP
