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

    /// Forward Declaration.
    class Iterator;

    MultiMapTB(const std::vector<Key>& equality_keys, const std::vector<Value>& sort_by_keys,
             unsigned num_partitions = 1024): equality_keys(equality_keys),
             sort_by_keys(sort_by_keys), num_partitions(num_partitions),
             mask(num_partitions - 1), partition_shift(__builtin_ctz(num_partitions)),
             thread_data(Partitions(num_partitions)),
             maps_per_partition(num_partitions) {
        //std::cout << "Num partitions: " << num_partitions << ", partition shift: " << partition_shift << std::endl;
        partition();
        combine_partitions();
    }

    MultiMapTB(const MultiMapTB& other):
        equality_keys(other.equality_keys),
        sort_by_keys(other.sort_by_keys),
        num_partitions(other.num_partitions),
        mask(other.mask),
        partition_shift(other.partition_shift),
        thread_data(Partitions()),
        maps_per_partition(other.maps_per_partition)
        /*partitioned_map(other.partitioned_map)*/ {}

    ~MultiMapTB() {
        tbb::parallel_invoke(
            [&] { maps_per_partition.clear(); },
            //[&] { partitioned_map.clear(); },
            //[&] { local_maps.clear(); },
            [&] { thread_data.clear(); }
        );
    }

    [[nodiscard]] inline Iterator begin() {
        size_t partition_idx = 0;
        while (partition_idx < maps_per_partition.size() &&
               maps_per_partition[partition_idx].empty()) {
            ++partition_idx;
        }

        if (partition_idx < maps_per_partition.size()) {
            return Iterator(this, partition_idx, maps_per_partition[partition_idx].begin());
        }

        return end();
        //return partitioned_map.begin();
    }

    [[nodiscard]] inline Iterator end() {
        return Iterator(this, maps_per_partition.size(), {});
        //return partitioned_map.end();
    }

    [[nodiscard]] inline bool contains(MapKey key) const {
        //return partitioned_map.contains(key);
        unsigned partition_idx = get_partition_index(key);
        return maps_per_partition[partition_idx].contains(key);
    }

    [[nodiscard]] inline size_t size() const {
        return -1;
        //return partitioned_map.size();
    }

    [[nodiscard]] [[gnu::always_inline]] std::vector<Entry>& operator[](MapKey key) {
        unsigned partition_idx = get_partition_index(key);
        //std::cout << "Partition idx: " << partition_idx << std::endl;
        return maps_per_partition[partition_idx][key];
        //return partitioned_map[key];
    }

    [[nodiscard]] size_t total_size_bytes() const {
        size_t total_size = sizeof(MultiMapTB);
        //for (auto& [k, v] : partitioned_map) {
        //    total_size += sizeof(k);
        //    total_size += v.size() * sizeof(Entry);
        //}
        return total_size;
    }

private:
    void partition() {
        tbb::blocked_range<size_t> range(0, equality_keys.size());
        tbb::parallel_for(range, [&](tbb::blocked_range<size_t>& local_range) {
            auto& local_partitions = thread_data.local();
            for (size_t i = local_range.begin(); i < local_range.end(); ++i) {
                unsigned pos = get_partition_index(equality_keys[i]);
                //std::cout << "Partition pos: " << pos << std::endl;
                local_partitions.partitions[pos].emplace_back(
                    equality_keys[i],
                    Entry(sort_by_keys[i], i));
            }
        });
    }

    void combine_partitions() {
        //std::atomic<size_t> total_size = 0;
        tbb::blocked_range<size_t> range(0, num_partitions);
        tbb::parallel_for(range, [&](tbb::blocked_range<size_t>& local_range) {
            for (size_t i = local_range.begin(); i < local_range.end(); ++i) {
                auto& local_map = maps_per_partition[i];

                for (auto& local_data : thread_data) {
                    for (auto& [key, entry] : local_data.partitions[i]) {
                        local_map[key].push_back(entry);
                    }
                }

                //total_size += local_map.size();
            }
        });

        //size_t global_num_buckets = next_power_of_two(total_size);


        //partitioned_map.reserve(total_size);
        //for (auto& local_map : maps_per_partition) {
        //    for (auto& [key, value]: local_map) {
        //        partitioned_map.insert({key, value.copy_tuples()});
        //    }
        //}
    }

    [[gnu::always_inline]] [[nodiscard]] unsigned get_partition_index(MapKey key) const {
        return hash(key) >> (64 - partition_shift);
    }

    inline uint32_t next_power_of_two(uint32_t v) {
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v++;
        return v;
    }

    struct Partitions {
        Partitions(): partitions(0) {}
        explicit Partitions(unsigned num_partitions): partitions(num_partitions) {}
        std::vector<TupleBuffer<std::pair<Key, Entry>>> partitions;
    };

    const std::vector<Key>& equality_keys;
    const std::vector<Value>& sort_by_keys;
    const unsigned num_partitions;
    const unsigned mask;
    const unsigned partition_shift;

    std::hash<MapKey> hash;

    tbb::enumerable_thread_specific<Partitions> thread_data;

    std::vector<std::unordered_map<MapKey, std::vector<Entry>>> maps_per_partition;
};

template<typename Entry>
class MultiMapTB<Entry>::Iterator {
public:
    /// Use the unordered_map iterator type as the base iterator for value, etc.
    using base_iterator = typename std::unordered_map<MapKey, std::vector<Entry>>::iterator;
    using iterator_category = std::forward_iterator_tag;
    using value_type = typename base_iterator::value_type;
    using difference_type = std::ptrdiff_t;
    using pointer = typename base_iterator::pointer;
    using reference = typename base_iterator::reference;

    Iterator(): parent(nullptr), partition_idx(0) {}

    Iterator(MultiMapTB* parent, size_t partition_idx, base_iterator iter)
        : parent(parent), partition_idx(partition_idx), current_iter(iter) {
        if (parent && partition_idx < parent->maps_per_partition.size() &&
            current_iter == parent->maps_per_partition[partition_idx].end()) {
            advance();
        }
    }

    Iterator& operator++() {
        ++current_iter;
        if (partition_idx < parent->maps_per_partition.size() &&
            current_iter == parent->maps_per_partition[partition_idx].end()) {
            advance();
        }
        return *this;
    }

    Iterator operator++(int) {
        Iterator tmp = *this;
        ++(*this);
        return tmp;
    }

    reference operator*() const {
        return *current_iter;
    }

    pointer operator->() const {
        return &(*current_iter);
    }

    bool operator==(const Iterator& other) const {
        return parent == other.parent &&
               partition_idx == other.partition_idx &&
               (partition_idx == parent->maps_per_partition.size() || current_iter == other.current_iter);
    }

    bool operator!=(const Iterator& other) const {
        return !(*this == other);
    }

private:
    /// Advances the iterator to the beginning of the next non-empty partition.
    void advance() {
        do {
            ++partition_idx;
            if (partition_idx < parent->maps_per_partition.size())
                current_iter = parent->maps_per_partition[partition_idx].begin();
        } while (partition_idx < parent->maps_per_partition.size() &&
                 current_iter == parent->maps_per_partition[partition_idx].end());
    }

    MultiMapTB* parent;
    size_t partition_idx;
    base_iterator current_iter;
};

#endif // ASOF_JOIN_PARALLEL_MULTI_MAP_HPP
