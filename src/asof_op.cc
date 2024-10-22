#include <iostream>
#include "relation.h"
#include <asof_join.h>

int main() {
    Prices prices = load_prices("../data/btc_usd_data.csv");
    //Prices prices = load_prices("../data/prices_small.csv");
    std::cout << "### FINISHED LOADING PRICES CSV ###" << std::endl;

    OrderBook order_book = load_order_book("../data/btc_orderbook_small.csv");
    //OrderBook order_book = load_order_book("../data/orderbook_small.csv");
    std::cout << "### FINISHED LOADING ORDERBOOK CSV ###" << std::endl;

    // uint64_t sum{0};
    // for (auto amount : orderbook.amounts) { sum += amount; }
    // std::cout << "TOTAL AMOUNT: " << sum << std::endl;

    BaselineASOFJoin asof_join(prices, order_book, LESS_EQUAL_THAN);
    ResultRelation baseline_result = asof_join.join();
    std::cout << "### FINISHED BASELINE ASOF ###" << std::endl;
    baseline_result.print();

    SortingASOFJoin sorting_asof_join(prices, order_book, LESS_EQUAL_THAN);
    ResultRelation sorting_result = sorting_asof_join.join();
    std::cout << "### FINISHED SORTING ASOF ###" << std::endl;

    PartitioningASOFJoin partitioning_asof_join(prices, order_book, LESS_EQUAL_THAN);
    ResultRelation partitioning_result = partitioning_asof_join.join();
    std::cout << "### FINISHED PARTITIONING ASOF ###" << std::endl;

    return 0;
}
