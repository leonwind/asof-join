#ifndef ASOF_JOIN_LOG_HPP
#define ASOF_JOIN_LOG_HPP

#include <iostream>
#include <string_view>


inline void log(std::string_view message) {
    #ifndef BENCHMARK_MODE
        std::cout << message << std::endl;
    #endif
}

#endif // ASOF_JOIN_LOG_HPP
