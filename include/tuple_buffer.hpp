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
        Buffer buffer;
        buffer.chunk = mmap(nullptr, BUFFER_SIZE, PROT_READ | PROT_WRITE,
                     MAP_SHARED, -1, 0);
        return std::make_unique<Buffer>(buffer);
    }

    ~Buffer() { munmap(chunk, BUFFER_SIZE); }

    char* alloc_tuple(size_t size) {
        char* ptr = reinterpret_cast<char*>(chunk) + bytes_used;
        bytes_used += size;
        return ptr;
    }

    [[nodiscard]] const char* begin() const {
        return reinterpret_cast<char*>(chunk);
    }

    [[nodiscard]] const char* end() const {
        return reinterpret_cast<char*>(chunk) + BUFFER_SIZE;
    }

    [[nodiscard]] inline bool free(size_t tuple_size) const {
        return tuple_size + bytes_used < BUFFER_SIZE;
    }
};

template <typename T>
class TupleBuffer {
    /// Fixed sized tuples
    const size_t tuple_size;
    std::unique_ptr<Buffer> curr;
    std::vector<std::unique_ptr<Buffer>> old_buffers;

public:
    TupleBuffer(): tuple_size(sizeof(T)), curr(Buffer::create()) {}

    char* store_tuple() {
        if (!curr->free(tuple_size)) [[unlikely]] {
            old_buffers.push_back(std::move(curr));
            curr = Buffer::create();
        }
        return curr->alloc_tuple(tuple_size);
    }

    [[nodiscard]] inline T operator[](size_t i) {
        return reinterpret_cast<T>(curr.get() + tuple_size * i);
    }
};

#endif // ASOF_JOIN_TUPLE_BUFFER_HPP
