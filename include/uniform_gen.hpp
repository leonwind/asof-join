#ifndef ASOF_JOIN_UNIFORM_GEN_HPP
#define ASOF_JOIN_UNIFORM_GEN_HPP

#include <string_view>
#include <random>
#include "fmt/format.h"

namespace uniform {
    inline std::string gen_stock_id(size_t num_stock_ids) {
        static thread_local std::mt19937 rng(std::random_device{}());
        std::uniform_int_distribution<std::size_t> dist(0, num_stock_ids);
        return fmt::format("s-{}", dist(rng));
    }

    inline std::size_t gen_int(size_t max) {
        static thread_local std::mt19937 rng(std::random_device{}());
        std::uniform_int_distribution<std::size_t> dist(0, max);
        return dist(rng);
    }
} // namespace uniform

#endif // ASOF_JOIN_UNIFORM_GEN_HPP