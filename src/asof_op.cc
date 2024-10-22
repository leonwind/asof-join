#include <iostream>
#include "relation.h"

int main() {
    Prices prices = load_prices("../data/btc_usd_data.csv");
    std::cout << "---FINISHED LOADING PRICES CSV---" << std::endl;

    Orderbook order_book = load_order_book("../data/btc_orderbook_small.csv");
    std::cout << "---FINISHED LOADING ORDERBOOK CSV---" << std::endl;

    // uint64_t sum{0};
    // for (auto amount : orderbook.amounts) { sum += amount; }
    // std::cout << "TOTAL AMOUNT: " << sum << std::endl;

    return 0;
}
