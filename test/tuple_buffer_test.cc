#include <gtest/gtest.h>
#include <iostream>
#include <vector>

#include "tuple_buffer.hpp"
#include "fmt/format.h"

namespace {
    struct Tuple {
        uint64_t tid;
        std::string payload;

        [[nodiscard]] std::string str() const {
            return fmt::format("tid={}, payload={}", tid, payload);
        }
    };
} // namespace

std::vector<Tuple> create_tuples(size_t num_tuples) {
    return {};
}

TEST(tuple_buffer, SingleTupleEntry) {
    TupleBuffer<Tuple> tuple_buffer;
    auto tuple = Tuple{0, "payload-0"};

    tuple_buffer.store_tuple(tuple);
    auto fetched_tuple = tuple_buffer[0];

    ASSERT_EQ(fetched_tuple.tid, tuple.tid);
    ASSERT_EQ(fetched_tuple.payload, tuple.payload);
}
