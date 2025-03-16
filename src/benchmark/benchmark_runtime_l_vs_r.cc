#include <iostream>
#include <vector>
#include <fmt/format.h>
#include "relation.hpp"
#include "benchmark.hpp"
#include "zipf_gen.hpp"
#include "uniform_gen.hpp"


#define PRICE_SAMPLING_RATE 30

namespace {
    Prices generate_random_prices(size_t num_prices) {
        std::vector<uint64_t> timestamps;
        std::vector<uint64_t> prices;
        std::vector<std::string> stock_ids;

        const size_t max_price = 100;
        const size_t num_diff_stocks = 128;

        for (size_t i = 0; i < num_prices; ++i) {
            timestamps.emplace_back(i * PRICE_SAMPLING_RATE);
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

    OrderBook generate_random_orderbook(size_t num_orders, size_t max_timestamp) {
        std::vector<uint64_t> timestamps;
        std::vector<uint64_t> amounts;
        std::vector<std::string> stock_ids;

        const size_t max_amount = 100;
        const size_t num_diff_stocks = 128;

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
} // namespace


void benchmarks::run_runtime_l_vs_r() {
    size_t num_runs = 3;
    size_t num_prices = 100'000'000;
    size_t num_positions = 100'000'000;

    size_t l_r_start = 1'000'000;
    size_t l_r_increase = 1'000'000;

    Prices prices = generate_random_prices(num_prices);
    OrderBook order_book = generate_random_orderbook(num_positions, num_prices * PRICE_SAMPLING_RATE);

    for (size_t l = l_r_start; l < num_prices; l += l_r_increase) {
        OrderBook order_book_subset = select_first_n_orders(order_book, /* n= */ l);

        for (size_t r = l_r_start; r < num_positions; r += l_r_increase) {
            Prices prices_subset = select_first_n_prices(prices, /* n= */ r);

            std::cout << fmt::format("l={}, r={}", l, r) << std::endl;
            benchmarks::run_benchmark(prices_subset, order_book_subset, num_runs);
        }
    }
}
