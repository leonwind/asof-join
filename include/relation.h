#ifndef ASOF_JOIN_RELATION_H
#define ASOF_JOIN_RELATION_H

#include <vector>
#include <string>
#include <cstdint>

struct Relation {
    std::vector<uint64_t> timestamps;
    std::vector<std::string> stock_ids;
    std::vector<uint64_t> prices;

    size_t size;
};

Relation load_data(std::string_view path, char delimiter = ',');

#endif //ASOF_JOIN_RELATION_H
