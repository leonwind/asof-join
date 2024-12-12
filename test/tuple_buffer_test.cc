#include <gtest/gtest.h>
#include <iostream>
#include <utility>
#include <vector>

#include "fmt/format.h"

#include "timer.hpp"
#include "tuple_buffer.hpp"


namespace {
    /// 64KB page size
    constexpr size_t BUFFER_SIZE = 65536;

    struct Tuple {
        uint64_t tid;
        std::string payload;

        Tuple(uint64_t tid, std::string payload): tid(tid), payload(std::move(payload)) {}

        Tuple(): tid(-1), payload() {}

        [[nodiscard]] std::string str() const {
            return fmt::format("tid={}, payload={}", tid, payload);
        }

        bool operator==(const Tuple& other) const {
            return tid == other.tid && payload == other.payload;
        }
    };
} // namespace

std::vector<Tuple> create_tuples(size_t num_tuples) {
    std::vector<Tuple> tuples(num_tuples);
    for (size_t i = 0; i < num_tuples; ++i) {
        tuples[i] = Tuple{i, fmt::format("payload-{}", i)};
    }
    return tuples;
}

TEST(tuple_buffer, SingleTupleEntry) {
    TupleBuffer<Tuple> tuple_buffer;
    auto tuple = Tuple{0, "payload-0"};

    tuple_buffer.store_tuple(tuple);
    auto fetched_tuple = tuple_buffer[0];

    ASSERT_EQ(tuple_buffer.size(), 1);
    ASSERT_EQ(fetched_tuple.tid, tuple.tid);
    ASSERT_EQ(fetched_tuple.payload, tuple.payload);
}

TEST(tuple_buffer, Store1FullBuffer) {
    size_t num_tuples = BUFFER_SIZE / sizeof(Tuple);
    auto tuples= create_tuples(num_tuples);
    TupleBuffer<Tuple> tuple_buffer;

    for (auto& tuple : tuples) {
        tuple_buffer.store_tuple(tuple);
    }

    ASSERT_EQ(tuple_buffer.size(), num_tuples);
    for (size_t i = 0; i < num_tuples; ++i) {
        auto fetched_tuple = tuple_buffer[i];
        ASSERT_EQ(fetched_tuple.tid, tuples[i].tid) << "Failed at index " << i;
        ASSERT_EQ(fetched_tuple.payload, tuples[i].payload) << "Failed at index " << i;
    }
}

TEST(tuple_buffer, Store2FullBuffers) {
    size_t num_tuples = 2 * BUFFER_SIZE / sizeof(Tuple);
    auto tuples= create_tuples(num_tuples);
    TupleBuffer<Tuple> tuple_buffer;

    for (auto& tuple : tuples) {
        tuple_buffer.store_tuple(tuple);
    }

    ASSERT_EQ(tuple_buffer.size(), num_tuples);
    for (size_t i = 0; i < num_tuples; ++i) {
        auto fetched_tuple = tuple_buffer[i];
        ASSERT_EQ(fetched_tuple.tid, tuples[i].tid) << "Failed at index " << i;
        ASSERT_EQ(fetched_tuple.payload, tuples[i].payload) << "Failed at index " << i;
    }
}

TEST(tuple_buffer, Store100FullBuffers) {
    size_t num_tuples = 100 * BUFFER_SIZE / sizeof(Tuple);
    auto tuples= create_tuples(num_tuples);
    TupleBuffer<Tuple> tuple_buffer;

    for (auto& tuple : tuples) {
        tuple_buffer.store_tuple(tuple);
    }

    ASSERT_EQ(tuple_buffer.size(), num_tuples);
    for (size_t i = 0; i < num_tuples; ++i) {
        auto fetched_tuple = tuple_buffer[i];
        ASSERT_EQ(fetched_tuple.tid, tuples[i].tid) << "Failed at index " << i;
        ASSERT_EQ(fetched_tuple.payload, tuples[i].payload) << "Failed at index " << i;
    }
}

TEST(tuple_buffer, IteratorOver100BuffersPreIncrement) {
    size_t num_tuples = 100 * BUFFER_SIZE / sizeof(Tuple);
    auto tuples= create_tuples(num_tuples);
    TupleBuffer<Tuple> tuple_buffer;

    for (auto& tuple : tuples) {
        tuple_buffer.store_tuple(tuple);
    }

    ASSERT_EQ(tuple_buffer.size(), num_tuples);
    size_t i = 0;
    for (auto iter = tuple_buffer.begin(); iter != tuple_buffer.end(); ++iter) {
        auto& fetched_tuple = *iter;
        ASSERT_EQ(fetched_tuple.tid, tuples[i].tid) << "Failed at index " << i;
        ASSERT_EQ(fetched_tuple.payload, tuples[i].payload) << "Failed at index " << i;
        ++i;
    }
    ASSERT_EQ(i, num_tuples);
}

TEST(tuple_buffer, IteratorOver100BuffersPostIncremenent) {
    size_t num_tuples = 100 * BUFFER_SIZE / sizeof(Tuple);
    auto tuples= create_tuples(num_tuples);
    TupleBuffer<Tuple> tuple_buffer;

    for (auto& tuple : tuples) {
        tuple_buffer.store_tuple(tuple);
    }

    ASSERT_EQ(tuple_buffer.size(), num_tuples);
    size_t i = 0;
    for (auto iter = tuple_buffer.begin(); iter != tuple_buffer.end(); iter++) {
        auto& fetched_tuple = *iter;
        ASSERT_EQ(fetched_tuple.tid, tuples[i].tid) << "Failed at index " << i;
        ASSERT_EQ(fetched_tuple.payload, tuples[i].payload) << "Failed at index " << i;
        ++i;
    }
    ASSERT_EQ(i, num_tuples);
}

TEST(tuple_buffer, IteratorOver100BuffersPointerAccess) {
    size_t num_tuples = 100 * BUFFER_SIZE / sizeof(Tuple);
    auto tuples= create_tuples(num_tuples);
    TupleBuffer<Tuple> tuple_buffer;

    for (auto& tuple : tuples) {
        tuple_buffer.store_tuple(tuple);
    }

    ASSERT_EQ(tuple_buffer.size(), num_tuples);
    size_t i = 0;
    for (auto iter = tuple_buffer.begin(); iter != tuple_buffer.end(); ++iter) {
        ASSERT_EQ(iter->tid, tuples[i].tid) << "Failed at index " << i;
        ASSERT_EQ(iter->payload, tuples[i].payload) << "Failed at index " << i;
        ++i;
    }
    ASSERT_EQ(i, num_tuples);
}

TEST(tuple_buffer, RangeIterationOver100Buffers) {
    size_t num_tuples = 100 * BUFFER_SIZE / sizeof(Tuple);
    auto tuples= create_tuples(num_tuples);
    TupleBuffer<Tuple> tuple_buffer;

    for (auto& tuple : tuples) {
        tuple_buffer.store_tuple(tuple);
    }

    ASSERT_EQ(tuple_buffer.size(), num_tuples);
    size_t i = 0;
    for (auto& fetched_tuple : tuple_buffer) {
        ASSERT_EQ(fetched_tuple.tid, tuples[i].tid) << "Failed at index " << i;
        ASSERT_EQ(fetched_tuple.payload, tuples[i].payload) << "Failed at index " << i;
        ++i;
    }
    ASSERT_EQ(i, num_tuples);
}

TEST(tuple_buffer, MaterializeIntoVector) {
    size_t num_tuples = 100 * BUFFER_SIZE / sizeof(Tuple);
    auto tuples= create_tuples(num_tuples);
    TupleBuffer<Tuple> tuple_buffer;

    for (auto& tuple : tuples) {
        tuple_buffer.store_tuple(tuple);
    }
    std::vector<Tuple> materialized_tuples = tuple_buffer.copy_tuples();

    ASSERT_EQ(materialized_tuples.size(), num_tuples);
    for (size_t i = 0; i < materialized_tuples.size(); ++i) {
        ASSERT_EQ(materialized_tuples[i].tid, tuples[i].tid) << "Failed at index " << i;
        ASSERT_EQ(materialized_tuples[i].payload, tuples[i].payload) << "Failed at index " << i;
    }
}

TEST(tuple_buffer, MoveConstructor) {
    size_t num_tuples = 100 * BUFFER_SIZE / sizeof(Tuple);
    auto tuples= create_tuples(num_tuples);
    TupleBuffer<Tuple> tuple_buffer;
    for (auto& tuple : tuples) {
        tuple_buffer.store_tuple(tuple);
    }

    TupleBuffer<Tuple> moved_tuple_constructor(std::move(tuple_buffer));

    for (size_t i = 0; i < num_tuples; ++i) {
        ASSERT_EQ(moved_tuple_constructor[i].tid, tuples[i].tid) << "Failed at index " << i;
        ASSERT_EQ(moved_tuple_constructor[i].payload, tuples[i].payload) << "Failed at index " << i;
    }
}

TEST(tuple_buffer, CopyConstructor) {
    size_t num_tuples = 100 * BUFFER_SIZE / sizeof(Tuple);
    auto tuples= create_tuples(num_tuples);
    TupleBuffer<Tuple> tuple_buffer;
    for (auto& tuple : tuples) {
        tuple_buffer.store_tuple(tuple);
    }

    TupleBuffer<Tuple> copied_tuple_buffer(tuple_buffer);

    for (size_t i = 0; i < num_tuples; ++i) {
        ASSERT_EQ(copied_tuple_buffer[i].tid, tuple_buffer[i].tid) << "Failed at index " << i;
        ASSERT_EQ(copied_tuple_buffer[i].payload, tuple_buffer[i].payload) << "Failed at index " << i;
    }
}

TEST(tuple_buffer, CopyNonTriviallyCopyableType) {
    using NonCpyTuple = std::pair<size_t, Tuple>;
    size_t num_tuples = 100 * BUFFER_SIZE / sizeof(NonCpyTuple);

    /// Use long string as payload to prevent short string copyable optimizations.
    size_t str_length = 1000;
    std::string long_string;
    for (size_t i = 0; i < str_length; ++i) {
        long_string += "0";
    }

    std::vector<NonCpyTuple> tuples;
    for (size_t i = 0; i < num_tuples; ++i) {
        tuples.emplace_back(i, Tuple{i, fmt::format("{}-{}", long_string, i)});
    }

    TupleBuffer<NonCpyTuple> tuple_buffer;

    for (auto& tuple : tuples) {
        tuple_buffer.store_tuple(tuple);
    }

    TupleBuffer<NonCpyTuple> copied_tuple_buffer(tuple_buffer);

    for (size_t i = 0; i < num_tuples; ++i) {
        ASSERT_EQ(copied_tuple_buffer[i].first, tuple_buffer[i].first) << "Failed at index " << i;
        ASSERT_EQ(copied_tuple_buffer[i].second, tuple_buffer[i].second) << "Failed at index " << i;
    }
}

TEST(tuple_buffer, Benchmark) {
    return;
    size_t num_tuples = 10000 * BUFFER_SIZE / sizeof(Tuple);

    std::vector<Tuple> tuple_vec;
    TupleBuffer<Tuple> tuple_buffer;
    Timer<std::chrono::milliseconds> timer;

    timer.start();
    for (size_t i = 0; i < num_tuples; ++i) {
        tuple_vec.emplace_back(i, fmt::format("payload-{}", i));
    }
    size_t vec_sum = 0;
    for (auto& tuple: tuple_vec) {
        vec_sum += tuple.tid;
    }
    auto vec_duration = timer.lap();

    for (size_t i = 0; i < num_tuples; ++i) {
        tuple_buffer.store_tuple(Tuple{i, fmt::format("payload-{}", i)});
    }
    auto materialized = tuple_buffer.copy_tuples();
    size_t tb_sum = 0;
    for (auto &tuple: materialized) {
        tb_sum += tuple.tid;
    }

    auto tb_duration = timer.lap();

    std::cout << "Sums: " << vec_sum << ", " << tb_sum << std::endl;
    std::cout << fmt::format("vec duration: {}, tb duration: {}", vec_duration, tb_duration) << std::endl;
}

