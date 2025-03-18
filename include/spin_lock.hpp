#ifndef ASOF_JOIN_SPIN_LOCK_HPP
#define ASOF_JOIN_SPIN_LOCK_HPP

#include <atomic>
#include "timer.hpp"

/// From https://anki3d.org/spinlock/
class SpinLock {

public:
    void lock() {
        //#ifndef BENCHMARK_MODE
        //    Timer<nanoseconds> timer;
        //    timer.start();
        //#endif

        while(lock_.test_and_set(std::memory_order_acquire)) {}

        //#ifndef BENCHMARK_MODE
        //    total_contended_duration += timer.stop();
        //#endif
    }

    void unlock() {
        lock_.clear(std::memory_order_release);
    }

    //[[nodiscard]] uint64_t get_contended_duration_ns() const {
    //    return total_contended_duration;
    //}

private:
    std::atomic_flag lock_ = ATOMIC_FLAG_INIT;

    //uint64_t total_contended_duration = 0;
};

#endif //ASOF_JOIN_SPIN_LOCK_HPP
