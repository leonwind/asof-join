#ifndef ASOF_JOIN_RELATION_H
#define ASOF_JOIN_RELATION_H

#include <vector>
#include <string>
#include <cstdint>

struct Relation {
    std::vector<uint64_t> time;
    std::vector<std::string> stock_ids;
    std::vector<uint64_t> price;

    size_t len;
};

#endif //ASOF_JOIN_RELATION_H
