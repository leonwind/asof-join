#include <iostream>
#include "relation.hpp"
#include "asof_join.hpp"
#include "timer.hpp"


void run_join(ASOFJoin& asof_op, size_t input_size, std::string_view strategy_name = "") {
    Timer timer;
    PerfEvent e;

    std::cout << "### START ASOF JOIN WITH [" << strategy_name << "] ###" << std::endl;

    timer.start();
    e.startCounters();
    auto result = asof_op.join();
    e.stopCounters();
    auto duration = timer.stop<std::chrono::milliseconds>();

    uint64_t total_sum = 0;
    for (auto value : result.values) { total_sum += value; }

    e.printReport(std::cout, input_size);
    std::cout << "### ASOF JOIN TOTAL VALUE SUM: " << total_sum << std::endl;
    std::cout << "### FINISHED ASOF JOIN WITH [" << strategy_name
        << "] IN " << duration << "[ms] ###" << std::endl;
}

std::pair<Prices, OrderBook> load_data(std::string_view prices_path, std::string_view positions_path,
        char delimiter = ',', bool shuffle = false) {
    Timer timer;
    timer.start();

    Prices prices = load_prices(prices_path, delimiter, shuffle);
    std::cout << "### FINISHED LOADING PRICES CSV ###" << std::endl;

    OrderBook order_book = load_order_book(positions_path, delimiter, shuffle);
    std::cout << "### FINISHED LOADING POSITIONS CSV ###" << std::endl;

    std::cout << "### FINISHED DATA LOADING IN " << timer.stop<milliseconds>() << "[ms] ###" << std::endl;
    std::cout << std::endl;

    return {prices, order_book};
}

int main() {
    auto [prices, order_book] = load_data(
        /* prices_path= */ "../data/zipf_prices.csv",
        /* positions_path= */"../data/zipf_positions.csv",
        /* delimiter= */ ',',
        /* shuffle= */ false);

    size_t input_size = prices.size + order_book.size;

    //SortingASOFJoin sorting_asof_join(prices, order_book, LESS_EQUAL_THAN, INNER);
    //run_join(sorting_asof_join, "sorted merge join") ;
    //std::cout << std::endl;

    //PartitioningLeftASOFJoin left_partitioning(prices, order_book, LESS_EQUAL_THAN, INNER);
    //run_join(left_partitioning, input_size, "partitioning left");
    //std::cout << std::endl;

    PartitioningRightASOFJoin right_partitioning(prices, order_book, LESS_EQUAL_THAN, INNER);
    run_join(right_partitioning, input_size, "partitioning right");

    return 0;
}
