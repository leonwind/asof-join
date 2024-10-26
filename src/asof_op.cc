#include <iostream>
#include "relation.hpp"
#include <asof_join.hpp>

int main() {
    Prices prices = load_prices("../data/btc_usd_data.csv");
    //Prices prices = load_prices("../data/prices_small.csv");
    std::cout << "### FINISHED LOADING PRICES CSV ###" << std::endl;

    OrderBook order_book = load_order_book("../data/btc_orderbook_medium.csv");
    //OrderBook order_book = load_order_book("../data/orderbook_small.csv");
    std::cout << "### FINISHED LOADING ORDERBOOK CSV ###" << std::endl;

    // BaselineASOFJoin baseline_asof_join(prices, order_book, LESS_EQUAL_THAN, INNER);
    // run_join(baseline_asof_join, "baseline");

    // SortingASOFJoin sorting_asof_join(prices, order_book, LESS_EQUAL_THAN, INNER);
    // run_join(baseline_asof_join, "sorting") ;

    PartitioningASOFJoin partitioning_asof_join(prices, order_book, LESS_EQUAL_THAN, INNER);
    run_join(partitioning_asof_join, "partitioning");

    return 0;
}
