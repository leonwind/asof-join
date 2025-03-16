#ifndef ASOF_JOIN_SEARCHES_HPP
#define ASOF_JOIN_SEARCHES_HPP

#include <vector>
#include <cstdint>

#include "asof_join.hpp"

template <typename T>
using IsJoinEntry = std::enable_if_t<std::is_base_of<ASOFJoin::JoinEntry, T>::value>;

namespace {
    inline size_t interpolate(uint64_t target, uint64_t first_val, uint64_t last_val, size_t n) {
        return ((target - first_val) * (n - 1)) / (last_val - first_val);
    }
} // namespace


namespace Search::Binary {
    /// Find the last value which is less or equal than [[target]].
    template <typename T, typename = IsJoinEntry<T>>
    inline T* less_equal_than(std::vector<T>& data, uint64_t target) {
        size_t left = 0;
        size_t right = data.size();

        while (left < right) {
            size_t middle = left + (right - left) / 2;

            bool is_valid_value = data[middle].get_key() <= target;
            left = is_valid_value ? middle + 1 : left;
            right = is_valid_value ? right : middle;
        }

        return (left > 0 && data[left - 1].get_key() <= target) ? &data[left - 1] : nullptr;
    }

    /// Find the first value which is greater or equal than [[target]].
    template <typename T, typename = IsJoinEntry<T>>
    inline T* greater_equal_than(std::vector<T>& data, uint64_t target) {
        size_t left = 0;
        size_t right = data.size();

        while (left < right) {
            size_t middle = left + (right - left) / 2;

            bool is_valid_value = data[middle].get_key() >= target;
            left = is_valid_value ? left : middle + 1;
            right = is_valid_value ? middle : right;
        }

        return (right < data.size() && data[right].get_key() >= target) ? &data[right] : nullptr;
    }
} // namespace Search::Binary


namespace Search::Exponential {
    /// Find the last value which is less or equal than [[target]].
    template <typename T, typename = IsJoinEntry<T>>
    inline T* less_equal_than(std::vector<T>& data, uint64_t target) {
        size_t n = data.size();
        if (n == 0) return nullptr;

        // Exponential search to find range.
        size_t bound = 1;
        while (bound < n && data[bound].get_key() <= target) {
            bound *= 2;
        }

        // Binary search within the found range.
        size_t left = bound / 2;
        size_t right = std::min(bound, n);

        while (left < right) {
            size_t middle = left + (right - left) / 2;

            bool is_valid_value = data[middle].get_key() <= target;
            left = is_valid_value ? middle + 1 : left;
            right = is_valid_value ? right : middle;
        }

        return (left > 0 && data[left - 1].get_key() <= target) ? &data[left - 1] : nullptr;
    }

    /// Find the first value which is greater or equal than [[target]].
    template <typename T, typename = IsJoinEntry<T>>
    inline T* greater_equal_than(std::vector<T>& data, uint64_t target) {
        size_t n = data.size();
        if (n == 0) return nullptr;

        // Exponential search to find range.
        size_t bound = 1;
        while (bound < n && data[bound].get_key() < target) {
            bound *= 2;
        }

        // Binary search within the found range.
        size_t left = bound / 2;
        size_t right = std::min(bound + 1, n);

        while (left < right) {
            size_t middle = left + (right - left) / 2;

            bool is_valid_value = data[middle].get_key() >= target;
            left = is_valid_value ? left : middle + 1;
            right = is_valid_value ? middle : right;
        }

        return (right < n && data[right].get_key() >= target) ? &data[right] : nullptr;
    }
} // namespace Search::Exponential


namespace Search::Interpolation {
    /// Find the last value which is less or equal than [[target]].
    template <typename T, typename = IsJoinEntry<T>>
    inline T* less_equal_than(std::vector<T>& data, uint64_t target) {
        size_t n = data.size();
        if (n == 0) return nullptr;

        // Run one interpolation step.
        size_t pos = 0;
        if (n > 1 && target >= data[0].get_key() && target <= data[n - 1].get_key()) {
            pos = interpolate(target, data[0].get_key(), data[n - 1].get_key(), n);
        }

        // Perform exponential search from the interpolated position.
        bool overestimated;
        size_t bound = 1;
        if (data[pos].get_key() >= target) {
            overestimated = true;
            /// while (bound - pos >= 0 && ...
            while (bound <= pos && data[pos - bound].get_key() >= target) {
                bound *= 2;
            }
        } else {
            overestimated = false;
            while (pos + bound < n && data[pos + bound].get_key() < target) {
                bound *= 2;
            }
        }

        size_t left = overestimated ? (pos > bound ? pos - bound : 0) : pos;
        size_t right = overestimated ? pos + 1 : std::min(pos + bound, n);

        while (left < right) {
            size_t middle = left + (right - left) / 2;

            bool is_valid_value = data[middle].get_key() <= target;
            left = is_valid_value ? middle + 1 : left;
            right = is_valid_value ? right : middle;
        }

        return (left > 0 && data[left - 1].get_key() <= target) ? &data[left - 1] : nullptr;
    }

    /// Find the first value which is greater or equal than [[target]].
    template <typename T, typename = IsJoinEntry<T>>
    inline T* greater_equal_than(std::vector<T>& data, uint64_t target) {
        size_t n = data.size();
        if (n == 0) return nullptr;

        // Run one interpolation step.
        size_t pos = 0;
        if (n > 1 && target >= data[0].get_key() && target <= data[n - 1].get_key()) {
            pos = interpolate(target, data[0].get_key(), data[n - 1].get_key(), n);
        }

        //std::cout << "Pos: " << pos << ": " << data[pos].get_key() << std::endl;

        // Perform exponential search from the interpolated position.
        bool overestimated;
        size_t bound = 1;
        if (data[pos].get_key() >= target) {
            overestimated = true;
            /// while (bound - pos >= 0 && ...
            while (bound < pos && data[pos - bound].get_key() >= target) {
                bound *= 2;
            }
        } else {
            overestimated = false;
            while (pos + bound < n && data[pos + bound].get_key() < target) {
                bound *= 2;
            }
        }

        // Binary search within the range found by exponential search.
        size_t left = overestimated ? (pos > bound ? pos - bound : 0) : pos;
        size_t right = overestimated ? pos + 1 : std::min(pos + bound, n);

        while (left < right) {
            size_t middle = left + (right - left) / 2;

            bool is_valid_value = data[middle].get_key() >= target;
            left = is_valid_value ? left : middle + 1;
            right = is_valid_value ? middle : right;
        }
        //std::cout << "Found: " << right << std::endl;
        return (right < n && data[right].get_key() >= target) ? &data[right] : nullptr;
    }
} // namespace Search::Interpolation

#endif // ASOF_JOIN_SEARCHES_HPP
