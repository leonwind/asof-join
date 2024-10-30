#include <iostream>
#include "relation.hpp"
#include "asof_join.hpp"
#include "timer.hpp"


void run_join(ASOFJoin& asof_op) {
    PerfEvent e;
    Timer timer;

    e.startCounters();
    timer.start();

    auto result = asof_op.join();

    e.stopCounters();
    auto duration = timer.stop<std::chrono::microseconds>();

    uint64_t total_sum = 0;
    for (auto value : result.values) { total_sum += value; }

    e.printReport(std::cout, result.size);
    std::cout << "### ASOF JOIN TOTAL VALUE SUM: " << total_sum << std::endl;
    result.print();
    std::cout << "### FINISHED ASOF JOIN WITH " << asof_op.STRATEGY_NAME
        << " IN " << duration << "[us] ###" << std::endl;
}

int main() {
    Prices prices = load_prices("../data/btc_usd_data.csv");
    //Prices prices = load_prices("../data/prices_small.csv");
    std::cout << "### FINISHED LOADING PRICES CSV ###" << std::endl;

    OrderBook order_book = load_order_book("../data/btc_orderbook_small.csv");
    // OrderBook order_book = load_order_book("../data/orderbook_small.csv");
    std::cout << "### FINISHED LOADING ORDERBOOK CSV ###" << std::endl;

    // BaselineASOFJoin baseline_asof_join(prices, order_book, LESS_EQUAL_THAN, INNER);
    // run_join(baseline_asof_join, "baseline");

    // SortingASOFJoin sorting_asof_join(prices, order_book, LESS_EQUAL_THAN, INNER);
    // run_join(baseline_asof_join, "sorting") ;

    PartitioningLeftASOFJoin left_partitioning(prices, order_book, LESS_EQUAL_THAN, INNER);
    run_join(left_partitioning);

    // PartitioningRightASOFJoin right_partitioning(prices, order_book, LESS_EQUAL_THAN, INNER);
    // run_join(right_partitioning);

    return 0;
}
