#ifndef ASOF_JOIN_PARALLEL_MULTI_MAP_HPP
#define ASOF_JOIN_PARALLEL_MULTI_MAP_HPP

#include <vector>
#include <iostream>

#include "tbb/enumerable_thread_specific.h"
#include "tbb/parallel_for.h"


template<typename Entry>
class MultiMap {
public:
    using Key = std::string;
    using Value = uint64_t;
    using MapKey = std::string_view;
    using iterator = std::unordered_map<MapKey, std::vector<Entry>>::iterator;

    MultiMap(const std::vector<Key>& equality_keys, const std::vector<Value>& sort_by_keys,
             size_t num_partitions = 1024):
             equality_keys(equality_keys), sort_by_keys(sort_by_keys), num_partitions(num_partitions),
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
        });

        /// TODO: Fix with HyperLogLog sketches.
        /// Should also be done parallel.
        partitioned_map.reserve(num_partitions);
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

#endif // ASOF_JOIN_PARALLEL_MULTI_MAP_HPP
