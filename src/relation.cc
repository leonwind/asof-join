#include <string>
#include <random>
#include "relation.hpp"
#include "mmap_file.hpp"
#include "uniform_gen.hpp"


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

uint64_t string_view_to_uint64_t(std::string_view strv) {
    uint64_t result{};

    auto [_, ec] = std::from_chars(strv.data(), strv.data() + strv.size(), result);

    if (ec != std::errc()) {
        throw std::runtime_error("Failed to parse uint64_t from string_view " + std::string(strv));
    }

    return result;
}

Prices load_prices(std::string_view path, char delimiter, bool shuffle) {
    MemoryMappedFile file(path);

    std::vector<uint64_t> timestamps;
    std::vector<std::string> stock_ids;
    std::vector<uint64_t> prices;
    std::string_view columns[3];

    for (auto iter = file.begin(), limit = file.end(); iter < limit;) {
        size_t column_idx = 0;
        for (auto last = iter;; ++iter) {
            if (*iter == delimiter) {
                std::string_view entry(last, iter - last);
                columns[column_idx++] = entry;
                last = iter + 1;
            } else if (*iter == '\n') {
                std::string_view entry(last, iter - last);
                columns[column_idx] = entry;
                ++iter;
                break;
            }
        }

        // Check if a header exists for the first row.
        // Currently, it only checks if the first field starts with "time".
        // If so, we assume that it is a header for our specific csv format.
        if (timestamps.empty() && columns[0].starts_with("time")) {
            continue;
        }

        timestamps.push_back(string_view_to_uint64_t(columns[0]));
        stock_ids.emplace_back(columns[1]);
        prices.push_back(string_view_to_uint64_t(columns[2]));
    }

    assert(timestamps.size() == stock_ids.size() &&
        stock_ids.size() == prices.size());

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

    return /* OrderBook= */ {
        /* timestamps= */ data.timestamps,
        /* stock_ids= */ data.stock_ids,
        /* amounts= */ data.prices,
        /* size= */ data.size
    };
}

Prices generate_equal_distributed_prices(size_t num_prices, size_t price_sampling_rate, size_t num_diff_stocks) {
    std::vector<uint64_t> timestamps;
    std::vector<uint64_t> prices;
    std::vector<std::string> stock_ids;

    const size_t max_price = 100;

    for (size_t i = 0; i < num_prices; ++i) {
        timestamps.emplace_back(i * price_sampling_rate);
        prices.emplace_back(uniform::gen_int(max_price));
        stock_ids.emplace_back(uniform::gen_stock_id(num_diff_stocks));
    }

    Prices result = {
        /* timestamps= */ timestamps,
        /* stock_ids= */ stock_ids,
        /* prices= */ prices,
        /* size= */ timestamps.size()
    };

    return shuffle_prices(result);
}


Prices generate_uniform_prices(size_t num_prices, size_t max_timestamp, size_t num_diff_stocks) {
    std::vector<uint64_t> timestamps;
    std::vector<uint64_t> prices;
    std::vector<std::string> stock_ids;

    const size_t max_price = 100;

    for (size_t i = 0; i < num_prices; ++i) {
        timestamps.emplace_back(uniform::gen_int(max_timestamp));
        prices.emplace_back(uniform::gen_int(max_price));
        stock_ids.emplace_back(uniform::gen_stock_id(num_diff_stocks));
    }

    return /* Prices= */ {
        /* timestamps= */ timestamps,
        /* stock_ids= */ stock_ids,
        /* prices= */ prices,
        /* size= */ timestamps.size()
    };
}


OrderBook generate_uniform_orderbook(size_t num_orders, size_t max_timestamp, size_t num_diff_stocks) {
    std::vector<uint64_t> timestamps;
    std::vector<uint64_t> amounts;
    std::vector<std::string> stock_ids;

    const size_t max_amount = 100;

    for (size_t i = 0; i < num_orders; ++i) {
        timestamps.emplace_back(uniform::gen_int(max_timestamp));
        amounts.emplace_back(uniform::gen_int(max_amount));
        stock_ids.emplace_back(uniform::gen_stock_id(num_diff_stocks));
    }

    return /* OrderBook= */ {
        /* timestamps= */ timestamps,
        /* stock_ids= */ stock_ids,
        /* amounts= */ amounts,
        /* size= */ timestamps.size()
    };
}

OrderBook generate_uniform_orderbook(size_t num_orders, size_t min_timestamp, size_t max_timestamp, size_t num_diff_stocks) {
    std::vector<uint64_t> timestamps;
    std::vector<uint64_t> amounts;
    std::vector<std::string> stock_ids;

    const size_t max_amount = 100;

    for (size_t i = 0; i < num_orders; ++i) {
        timestamps.emplace_back(uniform::gen_int(min_timestamp, max_timestamp));
        amounts.emplace_back(uniform::gen_int(max_amount));
        stock_ids.emplace_back(uniform::gen_stock_id(num_diff_stocks));
    }

    return /* OrderBook= */ {
        /* timestamps= */ timestamps,
        /* stock_ids= */ stock_ids,
        /* amounts= */ amounts,
        /* size= */ timestamps.size()
    };
}

Prices select_first_n_prices(Prices& prices_og, size_t n) {
    std::vector<uint64_t> timestamps(n);
    std::copy_n(prices_og.timestamps.begin(), n, timestamps.begin());

    std::vector<uint64_t> prices(n);
    std::copy_n(prices_og.prices.begin(), n, prices.begin());

    std::vector<std::string> stock_ids(n);
    std::copy_n(prices_og.stock_ids.begin(), n, stock_ids.begin());

    return /* Prices= */ {
        /* timestamps= */ timestamps,
        /* stock_ids= */ stock_ids,
        /* prices= */ prices,
        /* size= */ n
    };
}

OrderBook select_first_n_orders(OrderBook& order_book, size_t n) {
    std::vector<uint64_t> timestamps(n);
    std::copy_n(order_book.timestamps.begin(), n, timestamps.begin());

    std::vector<uint64_t> amounts(n);
    std::copy_n(order_book.amounts.begin(), n, amounts.begin());

    std::vector<std::string> stock_ids(n);
    std::copy_n(order_book.stock_ids.begin(), n, stock_ids.begin());

    return /* OrderBook= */ {
        /* timestamps= */ timestamps,
        /* stock_ids= */ stock_ids,
        /* amounts= */ amounts,
        /* size= */ n
    };
}
