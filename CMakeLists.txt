cmake_minimum_required(VERSION 3.15)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project(asof-join)

include(lib/gtest.cmake)
find_package(Threads REQUIRED)

# Set compile flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wno-unused-parameter")
set(CMAKE_CXX_FLAGS_DEBUG "-g -fsanitize=address,leak,undefined -O3")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

include_directories(include)
add_library(asof_join src/relation.cc src/asof_join.cpp)

# Tests
add_executable(test_all test/asof_join.cpp)
target_link_libraries(test_all asof_join gtest gtest_main)

add_executable(asofOp src/asof_op.cc)
target_link_libraries(asofOp asof_join)
