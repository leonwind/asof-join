#include <iostream>
#include "relation.h"
#include <asof_join.h>

int main() {
    Prices prices = load_prices("../data/btc_usd_data.csv");
    // Prices prices = load_prices("../data/prices_small.csv");
    std::cout << "### FINISHED LOADING PRICES CSV ###" << std::endl;

    OrderBook order_book = load_order_book("../data/btc_orderbook_medium.csv");
    // OrderBook order_book = load_order_book("../data/orderbook_small.csv");
    std::cout << "### FINISHED LOADING ORDERBOOK CSV ###" << std::endl;

    // uint64_t sum{0};
    // for (auto amount : orderbook.amounts) { sum += amount; }
    // std::cout << "TOTAL AMOUNT: " << sum << std::endl;

    // BaselineASOFJoin asof_join(prices, order_book, LESS_EQUAL_THAN, INNER);
    // auto baseline_timer = Timer::start();
    // ResultRelation baseline_result = asof_join.join();
    // auto baseline_duration = baseline_timer.end();
    // baseline_result.print();
    // std::cout << "### FINISHED BASELINE ASOF IN " << baseline_duration
    //    << "[us] ###" << std::endl;

    // SortingASOFJoin sorting_asof_join(prices, order_book, LESS_EQUAL_THAN, INNER);
    // ResultRelation sorting_result = sorting_asof_join.join();
    // sorting_result.print();
    // std::cout << "### FINISHED SORTING ASOF ###" << std::endl;

    // PartitioningASOFJoin partitioning_asof_join(prices, order_book, LESS_EQUAL_THAN, INNER);
    // auto partitioning_timer = Timer::start();
    // ResultRelation partitioning_result = partitioning_asof_join.join();
    // auto partitioning_duration = partitioning_timer.end();
    // partitioning_result.print();
    // std::cout << "### FINISHED PARTITIONING ASOF IN " << partitioning_duration
    //     << "[us] ###" << std::endl;

    PartitioningASOFJoin partitioning_asof_join(prices, order_book, LESS_EQUAL_THAN, INNER);
    run_join(partitioning_asof_join, "partitioning") ;

    return 0;
}
