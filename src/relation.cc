#include <string>
#include "relation.hpp"
#include "mmap_file.hpp"

Prices load_prices(std::string_view path, char delimiter) {
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

    return {
        /* timestamps= */ timestamps,
        /* stock_ids= */ stock_ids,
        /* prices= */ prices,
        /* size= */ timestamps.size()
    };
}

OrderBook load_order_book(std::string_view path, char delimiter) {
    auto data = load_prices(path, delimiter);
    return {
        /* timestamps= */ data.timestamps,
        /* stock_ids= */ data.stock_ids,
        /* amounts= */ data.prices,
        /* size= */ data.size
    };
}
