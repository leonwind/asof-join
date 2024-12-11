#include <gtest/gtest.h>
#include <iostream>
#include <utility>
#include <vector>

#include "fmt/format.h"

#include "timer.hpp"
#include "tuple_buffer.hpp"


namespace {
    constexpr size_t BUFFER_SIZE = 4096;

    struct Tuple {
        uint64_t tid;
        std::string payload;

        Tuple(uint64_t tid, std::string payload): tid(tid), payload(std::move(payload)) {}

        Tuple(): tid(-1), payload() {}

        [[nodiscard]] std::string str() const {
            return fmt::format("tid={}, payload={}", tid, payload);
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
    auto tuples = create_tuples(num_tuples);
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
    auto tuples = create_tuples(num_tuples);
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
    auto tuples = create_tuples(num_tuples);
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
    auto tuples = create_tuples(num_tuples);
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
    auto tuples = create_tuples(num_tuples);
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
    auto tuples = create_tuples(num_tuples);
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
    auto tuples = create_tuples(num_tuples);
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

TEST(tuple_buffer, Benchmark) {
    return;
    size_t num_tuples = 100000 * BUFFER_SIZE / sizeof(Tuple);

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
    Timer<std::chrono::milliseconds> steps;

    steps.start();
    for (size_t i = 0; i < num_tuples; ++i) {
        tuple_buffer.store_tuple(Tuple{i, fmt::format("payload-{}", i)});
    }
    auto insert_duration = steps.lap();
    size_t tb_sum = 0;
    for (auto &tuple: tuple_buffer) {
        tb_sum += tuple.tid;
    }
    auto scan_duration = steps.lap();
    auto tb_duration = timer.lap();

    std::cout << "Sums: " << vec_sum << ", " << tb_sum << std::endl;
    std::cout << fmt::format("vec duration: {}, tb duration: {}", vec_duration, tb_duration) << std::endl;
    std::cout << fmt::format("Insert: {}, Scan: {}", insert_duration, scan_duration) << std::endl;
}