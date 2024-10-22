import csv
import random


def generate_random_timestamp(earliest, latest):
    return random.randint(earliest, latest)


def generate_random_amount(max_amount):
    return random.randint(0, max_amount)


def get_random_stock_id(stock_ids):
    return random.choice(stock_ids)


def write_csv(num_orders, stock_ids, earliest_time, latest_time, max_amount, file_name):
    with open(file_name, "w") as file:
        writer = csv.writer(file)

        for _ in range(num_orders):
            timestamp = generate_random_timestamp(earliest_time, latest_time)
            stock_id = get_random_stock_id(stock_ids)
            amount = generate_random_amount(max_amount)
            writer.writerow((timestamp, stock_id, amount))


def generate_btc_orderbook():
    write_csv(
        num_orders=10000,
        stock_ids=["BTC"],
        earliest_time=1325412060,
        latest_time=1729122660,
        max_amount=10,
        file_name="../data/btc_orderbook_medium.csv")


if __name__ == "__main__":
    generate_btc_orderbook()
