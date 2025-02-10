#ifndef ASOF_JOIN_PARALLEL_MULTI_MAP_HPP
#define ASOF_JOIN_PARALLEL_MULTI_MAP_HPP

#include <vector>
#include <iostream>

#include "tbb/enumerable_thread_specific.h"
#include "tbb/parallel_invoke.h"
#include "tbb/parallel_for.h"
#include "tbb/parallel_for_each.h"

#include "tuple_buffer.hpp"

template<typename Entry>
class MultiMap {
public:
    using Key = std::string;
    using Value = uint64_t;
    using MapKey = std::string_view;
    using iterator = std::unordered_map<MapKey, std::vector<Entry>>::iterator;

    MultiMap(const std::vector<Key>& equality_keys, const std::vector<Value>& sort_by_keys,
             size_t num_partitions = 1024): equality_keys(equality_keys),
             sort_by_keys(sort_by_keys), num_partitions(num_partitions),
             mask(num_partitions - 1), thread_data(Partitions(num_partitions)) {
        partition();
        combine_partitions();
    }

    [[nodiscard]] inline iterator begin() {
        return partitioned_map.begin();
    }

    [[nodiscard]] inline iterator end() {
        return partitioned_map.end();
    }

    [[nodiscard]] inline bool contains(MapKey key) {
        return partitioned_map.contains(key);
    }

    [[nodiscard]] inline size_t size() {
        return partitioned_map.size();
    }

    [[nodiscard]] inline std::vector<Entry>& operator[](MapKey key) {
        return partitioned_map[key];
    }

private:
    void partition() {
        tbb::blocked_range<size_t> range(0, equality_keys.size());
        tbb::parallel_for(range, [&](tbb::blocked_range<size_t>& local_range) {
            auto& local_partitions = thread_data.local();
            for (size_t i = local_range.begin(); i < local_range.end(); ++i) {
                size_t pos = hash(equality_keys[i]) & mask;
                local_partitions.partitions[pos].emplace_back(equality_keys[i], Entry(sort_by_keys[i], i));
            }
        });
    }

    void combine_partitions() {
        std::atomic<size_t> total_size;
        tbb::blocked_range<size_t> range(0, num_partitions);
        tbb::parallel_for(range, [&](tbb::blocked_range<size_t>& local_range) {
            auto& local_map = local_maps.local();
            for (auto& local_data : thread_data) {
                for (size_t i = local_range.begin(); i < local_range.end(); ++i) {
                    for (auto& [key, entry] : local_data.partitions[i]) {
                        local_map[key].push_back(entry);
                    }
                }
            }
            total_size += local_map.bucket_count();
        });

        partitioned_map.reserve(total_size);
        for (auto& local_map : local_maps) {
            partitioned_map.merge(local_map);
        }
    }

    struct Partitions {
        explicit Partitions(size_t num_partitions): partitions(num_partitions) {}
        std::vector<std::vector<std::pair<Key, Entry>>> partitions;
    };

    const std::vector<Key>& equality_keys;
    const std::vector<Value>& sort_by_keys;
    const size_t num_partitions;
    const uint64_t mask;

    std::hash<Key> hash;

    tbb::enumerable_thread_specific<Partitions> thread_data;
    tbb::enumerable_thread_specific<std::unordered_map<MapKey, std::vector<Entry>>> local_maps;

    std::unordered_map<MapKey, std::vector<Entry>> partitioned_map;
};


template<typename Entry>
class MultiMapTB {
public:
    using Key = std::string;
    using Value = uint64_t;
    using MapKey = std::string_view;
    using iterator = std::unordered_map<MapKey, std::vector<Entry>>::iterator;

    MultiMapTB(const std::vector<Key>& equality_keys, const std::vector<Value>& sort_by_keys,
             size_t num_partitions = 1024): equality_keys(equality_keys),
             sort_by_keys(sort_by_keys), num_partitions(num_partitions),
             mask(num_partitions - 1), thread_data(Partitions(num_partitions)) {
        partition();
        combine_partitions();
    }

    MultiMapTB(const MultiMapTB& other):
        equality_keys(other.equality_keys),
        sort_by_keys(other.sort_by_keys),
        num_partitions(other.num_partitions),
        mask(other.mask),
        thread_data(Partitions()),
        local_maps(),
        partitioned_map(other.partitioned_map) {}


    ~MultiMapTB() {
        tbb::parallel_invoke(
            [&] { partitioned_map.clear(); },
            [&] { local_maps.clear(); },
            [&] { thread_data.clear(); }
    );

}

    [[nodiscard]] inline iterator begin() {
        return partitioned_map.begin();
    }

    [[nodiscard]] inline iterator end() {
        return partitioned_map.end();
    }

    [[nodiscard]] inline bool contains(MapKey key) const {
        return partitioned_map.contains(key);
    }

    [[nodiscard]] inline size_t size() const {
        return partitioned_map.size();
    }

    [[nodiscard]] inline std::vector<Entry>& operator[](MapKey key) {
        return partitioned_map[key];
    }

    [[nodiscard]] size_t total_size_bytes() const {
        size_t total_size = sizeof(MultiMapTB);
        for (auto& [k, v] : partitioned_map) {
            total_size += sizeof(k);
            total_size += v.size() * sizeof(Entry);
        }
        return total_size;
    }

private:
    void partition() {
        tbb::blocked_range<size_t> range(0, equality_keys.size());
        tbb::parallel_for(range, [&](tbb::blocked_range<size_t>& local_range) {
            auto& local_partitions = thread_data.local();
            for (size_t i = local_range.begin(); i < local_range.end(); ++i) {
                size_t pos = hash(equality_keys[i]) & mask;
                local_partitions.partitions[pos].emplace_back(
                    equality_keys[i],
                    Entry(sort_by_keys[i], i));
            }
        });
    }

    void combine_partitions() {
        std::atomic<size_t> total_size = 0;
        tbb::blocked_range<size_t> range(0, num_partitions);
        tbb::parallel_for(range, [&](tbb::blocked_range<size_t>& local_range) {
            auto& local_map = local_maps.local();
            for (auto& local_data : thread_data) {
                for (size_t i = local_range.begin(); i < local_range.end(); ++i) {
                    for (auto& [key, entry] : local_data.partitions[i]) {
                        local_map[key].emplace_back(entry);
                    }
                }
            }
            total_size += local_map.bucket_count();
        });

        partitioned_map.reserve(total_size);
        for (auto& local_map : local_maps) {
            for (auto& [key, value]: local_map) {
                partitioned_map.insert({key, value.copy_tuples()});
            }
        }
    }

    struct Partitions {
        Partitions(): partitions(0) {}
        explicit Partitions(size_t num_partitions): partitions(num_partitions) {}
        std::vector<TupleBuffer<std::pair<Key, Entry>>> partitions;
    };

    const std::vector<Key>& equality_keys;
    const std::vector<Value>& sort_by_keys;
    const size_t num_partitions;
    const uint64_t mask;

    std::hash<Key> hash;

    tbb::enumerable_thread_specific<Partitions> thread_data;
    tbb::enumerable_thread_specific<std::unordered_map<MapKey, TupleBuffer<Entry>>> local_maps;

    std::unordered_map<MapKey, std::vector<Entry>> partitioned_map;
};

#endif // ASOF_JOIN_PARALLEL_MULTI_MAP_HPP
