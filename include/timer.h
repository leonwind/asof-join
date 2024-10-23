#ifndef ASOF_JOIN_TIMER_H
#define ASOF_JOIN_TIMER_H

#include <chrono>
#include <cstdint>

using namespace std::chrono;

class Timer {

public:
    static Timer start() { return {}; }

    template<typename unit = microseconds> uint64_t end() {
        steady_clock::time_point end_time = steady_clock::now();
        return duration_cast<unit>(end_time - start_time_).count();
    }

private:
    Timer(): start_time_(steady_clock::now()) {}
    steady_clock::time_point start_time_;
};

#endif //ASOF_JOIN_TIMER_H
