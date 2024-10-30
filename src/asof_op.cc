#include <iostream>
#include "relation.hpp"
#include "asof_join.hpp"
#include "timer.hpp"


void run_join(ASOFJoin& asof_op, std::string_view strategy_name = "") {
    // PerfEvent e;
    Timer timer;

    //e.startCounters();
    timer.start();

    auto result = asof_op.join();

    //e.stopCounters();
    auto duration = timer.stop();

    uint64_t total_sum = 0;
    for (auto value : result.values) { total_sum += value; }

    //e.printReport(std::cout, result.size);
    std::cout << "\n### ASOF JOIN TOTAL VALUE SUM: " << total_sum << std::endl;
    //result.print();
    std::cout << "### FINISHED ASOF JOIN WITH " << strategy_name
        << " IN " << duration << "[us] ###" << std::endl;
}

int main() {
    Prices prices = load_prices("../data/btc_usd_data.csv", ',', true);
    //Prices prices = load_prices("../data/prices_small.csv");
    std::cout << "### FINISHED LOADING PRICES CSV ###" << std::endl;

    OrderBook order_book = load_order_book("../data/btc_orderbook_medium.csv", ',', true);
    //OrderBook order_book = load_order_book("../data/orderbook_small.csv");
    std::cout << "### FINISHED LOADING ORDERBOOK CSV ###" << std::endl;

    //SortingASOFJoin sorting_asof_join(prices, order_book, LESS_EQUAL_THAN, INNER);
    //run_join(sorting_asof_join, "sorted merge join") ;

    PartitioningLeftASOFJoin left_partitioning(prices, order_book, LESS_EQUAL_THAN, INNER);
    run_join(left_partitioning, "partitioning + binary search");

    PartitioningRightASOFJoin right_partitioning(prices, order_book, LESS_EQUAL_THAN, INNER);
    run_join(right_partitioning, "partitioning right");

    return 0;
}
