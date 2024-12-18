#ifndef ASOF_JOIN_UTIL_HPP
#define ASOF_JOIN_UTIL_HPP

#include <cstdint>

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

inline constexpr uint64_t next_pow2_32(uint32_t v) {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v++;
    return v;
}

#endif //ASOF_JOIN_UTIL_HPP
