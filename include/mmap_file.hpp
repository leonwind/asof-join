#ifndef ASOF_JOIN_MMAP_FILE_HPP
#define ASOF_JOIN_MMAP_FILE_HPP

#include <iostream>
#include <string>
#include <charconv>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <bits/stdc++.h>

class MemoryMappedFile {
    size_t size;

public:
    explicit MemoryMappedFile(std::string_view path) {
        handle = ::open(path.begin(), O_RDONLY);
        lseek(handle, 0, SEEK_END);
        size = lseek(handle, 0, SEEK_CUR);
        mapping = mmap(nullptr, size, PROT_READ, MAP_SHARED, handle, 0);
    }

    ~MemoryMappedFile() {
        munmap(mapping, size);
        close(handle);
    }

    [[nodiscard]] const char* begin() const {
        return static_cast<char*>(mapping);
    }

    [[nodiscard]] const char* end() const {
        return static_cast<char*>(mapping) + size;
    }

private:
    int handle;
    void* mapping;
};

#endif //ASOF_JOIN_MMAP_FILE_HPP
