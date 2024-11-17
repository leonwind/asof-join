#ifndef ASOF_JOIN_TIMER_HPP
#define ASOF_JOIN_TIMER_HPP

#include <chrono>
#include <cstdint>

using namespace std::chrono;

template<typename T = microseconds>
class Timer {

public:
    Timer() = default;

    void start() {
        steady_clock::time_point curr_time = steady_clock::now();
        start_time_ = curr_time;
        lap_time_ = curr_time;
    }

    template<typename unit = T> uint64_t stop() {
        steady_clock::time_point end_time = steady_clock::now();
        return duration_cast<unit>(end_time - start_time_).count();
    }

    template<typename unit = T> uint64_t lap() {
        steady_clock::time_point curr_time = steady_clock::now();
        auto duration = duration_cast<unit>(curr_time - lap_time_).count();
        lap_time_ = curr_time;
        return duration;
    }

    std::string_view unit() {
        if (std::is_same<T, seconds>::value) {
            return "[s]";
        }
        if (std::is_same<T, milliseconds>::value) {
            return "[ms]";
        }
        if (std::is_same<T, microseconds>::value) {
            return "[us]";
        }
        if (std::is_same<T, nanoseconds>::value) {
            return "[ns]";
        }

        return "[]";
    }

private:
    steady_clock::time_point start_time_;
    steady_clock::time_point lap_time_;
};

#endif //ASOF_JOIN_TIMER_HPP
