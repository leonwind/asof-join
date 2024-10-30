#include <string>
#include <random>
#include "relation.hpp"
#include "mmap_file.hpp"

Prices shuffle_prices(Prices& prices) {
    std::vector<size_t> indices(prices.size);
    for (size_t i = 0; i < prices.size; ++i) { indices[i] = i; }

    std::mt19937 g(0);
    std::shuffle(indices.begin(), indices.end(), g);

    std::vector<uint64_t> timestamps_tmp(prices.size);
    std::vector<std::string> stock_ids_tmp(prices.size);
    std::vector<uint64_t> prices_tmp(prices.size);

    for (size_t i = 0; i < prices.size; ++i) {
        timestamps_tmp[i] = prices.timestamps[indices[i]];
        stock_ids_tmp[i] = prices.stock_ids[indices[i]];
        prices_tmp[i] = prices.prices[indices[i]];
    }

    return {timestamps_tmp, stock_ids_tmp, prices_tmp, prices.size};
}

Prices load_prices(std::string_view path, char delimiter, bool shuffle) {
    MemoryMappedFile file(path);

    std::vector<uint64_t> timestamps;
    std::vector<std::string> stock_ids;
    std::vector<uint64_t> prices;
    std::string columns[3];

    for (auto iter = file.begin(), limit = file.end(); iter < limit;) {
        size_t column_idx = 0;
        for (auto last = iter;; ++iter) {
            if (*iter == delimiter) {
                const std::string entry(last, iter - last);
                columns[column_idx++] = entry;
                last = iter + 1;
            } else if (*iter == '\n') {
                const std::string entry(last, iter - last);
                columns[column_idx] = entry;
                ++iter;
                break;
            }
        }

        timestamps.push_back(std::stoull(columns[0]));
        stock_ids.push_back(columns[1]);
        prices.push_back(std::stoull(columns[2]));
    }

    Prices result = {
        /* timestamps= */ timestamps,
        /* stock_ids= */ stock_ids,
        /* prices= */ prices,
        /* size= */ timestamps.size()
    };

    return shuffle ? shuffle_prices(result) : result;
}

OrderBook load_order_book(std::string_view path, char delimiter, bool shuffle) {
    auto data = load_prices(path, delimiter, shuffle);

    return {
        /* timestamps= */ data.timestamps,
        /* stock_ids= */ data.stock_ids,
        /* amounts= */ data.prices,
        /* size= */ data.size
    };
}
