cmake_minimum_required(VERSION 3.15)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project(asof-join)

include(lib/gtest.cmake)
find_package(Threads REQUIRED)

# Set compile flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wno-unused-parameter")
set(CMAKE_CXX_FLAGS_DEBUG "-Wextra -g -fsanitize=address")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

include_directories(include)
add_library(asof_join src/AsofOp.cc src/Relation.cc)

# Tests
# add_executable(test_all test/WeeDB.cpp)
# target_link_libraries(test_all wee_db gtest gtest_main pthread)

add_executable(asofOp src/AsofOp.cc)
target_link_libraries(asofOp asof_join)
