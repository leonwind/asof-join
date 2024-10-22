#include <iostream>
#include "relation.h"

int main() {
    Relation relation = load_data("../data/btc_usd_data.csv");
    std::cout << "---FINISHED LOADING CSV---" << std::endl;

    return 0;
}