#include <iostream>
#include <sys/wait.h>
#include "relation.hpp"
#include "asof_join.hpp"
#include "timer.hpp"
#include "tbb/global_control.h"


std::pair<Prices, OrderBook> load_data(
        std::string_view prices_path,
        std::string_view positions_path,
        char delimiter = ',',
        bool shuffle = false) {
    Timer timer;
    timer.start();

    Prices prices = load_prices(prices_path, delimiter, shuffle);
    std::cout << "### FINISHED LOADING PRICES CSV ###" << std::endl;

    OrderBook order_book = load_order_book(positions_path, delimiter, shuffle);
    std::cout << "### FINISHED LOADING POSITIONS CSV ###" << std::endl;

    std::cout << "### FINISHED DATA LOADING IN " << timer.stop<milliseconds>() << "[ms] ###" << std::endl;
    std::cout << "Prices num rows: " << prices.timestamps.size() << std::endl;
    std::cout << "Positions num rows: " << order_book.timestamps.size() << std::endl;

    return {std::move(prices), std::move(order_book)};
}

void run_join_in_new_process(ASOFJoin& asof_op) {
    /// Since TBB is keeping the internal thread pool alive and there is no way
    /// to completely killing it, we start a new process to force a new thread pool.
    /// Otherwise, PerfEvent is not able to detect the correct number of CPU utilization since
    /// the threads are created before Perf.
    pid_t pid = fork();
    if (pid == 0) {
        //tbb::global_control control(tbb::global_control::max_allowed_parallelism, 1);
        asof_op.join();
        uint64_t total_val = asof_op.result.value_sum;
        std::cout << "Total value: " << total_val << std::endl;
        std::cout << "Num rows: " << asof_op.result.size << std::endl;
        _exit(0);
    } else {
        wait(nullptr);
    }
}

void run_join(ASOFJoin& asof_op, size_t input_size, std::string_view strategy_name = "") {
    Timer timer;
    PerfEvent e;

    std::cout << std::endl;
    std::cout << "### START ASOF JOIN WITH [" << strategy_name << "] ###" << std::endl;

    timer.start();
    e.startCounters();

    //run_join_in_new_process(asof_op);
    asof_op.join();
    uint64_t total_val = asof_op.result.value_sum;
    std::cout << "Total value: " << total_val << std::endl;
    std::cout << "Num rows: " << asof_op.result.size << std::endl;
    auto duration = timer.stop<std::chrono::milliseconds>();

    e.stopCounters();
    e.printReport(std::cout, input_size);

    std::cout << "### FINISHED ASOF JOIN WITH [" << strategy_name
        << "] IN " << duration << "[ms] ###" << std::endl;
}

int main() {
    auto [prices, order_book] = load_data(
        ///* prices_path= */ "../data/btc_usd_data.csv",
        ///* positions_path= */"../data/btc_orderbook_large.csv",
        /* prices_path= */ "../data/zipf_prices.csv",
        /* positions_path= */"../data/zipf_1_5_positions_2000000.csv",
        /* delimiter= */ ',',
        /* shuffle= */ false);
    size_t input_size = prices.size + order_book.size;

    PartitioningSortedMergeJoin partition_sort(prices, order_book, LESS_EQUAL_THAN, INNER);
    //run_join(partition_sort, input_size, "partitioning sort");

    PartitioningLeftASOFJoin left_partitioning(prices, order_book, LESS_EQUAL_THAN, INNER);
    run_join(left_partitioning, input_size, "partitioning left");

    PartitioningRightASOFJoin right_partitioning(prices, order_book, LESS_EQUAL_THAN, INNER);
    run_join(right_partitioning, input_size, "partitioning right");

    PartitioningBothSortLeftASOFJoin partitioning_both(prices, order_book, LESS_EQUAL_THAN, INNER);
    run_join(partitioning_both, input_size, "partitioning both + sort left");

    return 0;
}
