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
        auto buffer = std::unique_ptr<Buffer>(new Buffer());
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

    [[nodiscard]] const char* begin() const {
        return reinterpret_cast<char*>(chunk);
    }

    [[nodiscard]] const char* end() const {
        return reinterpret_cast<char*>(chunk) + BUFFER_SIZE;
    }

    [[nodiscard]] char* data() {
        return reinterpret_cast<char*>(chunk);
    }

    [[nodiscard]] const char* data() const {
        return reinterpret_cast<const char*>(chunk);
    }

    [[nodiscard]] inline size_t get_bytes_usd() const { return bytes_used; }
};

template <typename T>
class TupleBuffer {
    /// Fixed sized tuples
    const size_t tuple_size;
    std::unique_ptr<Buffer> curr;
    std::vector<std::unique_ptr<Buffer>> old_buffers;
    size_t num_tuples;

public:
    TupleBuffer(): tuple_size(sizeof(T)), curr(Buffer::create()), old_buffers(), num_tuples(0) {}

    char* get_tuple_slot() {
        ++num_tuples;
        if (!curr->has_space(tuple_size)) [[unlikely]] {
            old_buffers.push_back(std::move(curr));
            curr = Buffer::create();
        }
        return curr->alloc_tuple(tuple_size);
    }

    inline void store_tuple(T tuple) {
        ++num_tuples;
        auto* memory = (T*) get_tuple_slot();
        *memory = tuple;
    }

    [[nodiscard]] inline size_t size() const {
        return num_tuples;
    }

    [[nodiscard]] inline T operator[](size_t i) const {
        return *reinterpret_cast<T*>(curr->data() + tuple_size * i);
    }
};

#endif // ASOF_JOIN_TUPLE_BUFFER_HPP
