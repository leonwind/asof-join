import csv


def read_and_delete_csv(source_path, target_path):
    with open(source_path, "r") as source:
        reader = csv.reader(source)
 
        # Skip the header
        next(reader, None)

        with open(target_path, "w") as target:
            writer = csv.writer(target)

            for row in reader:
                timestamp = int(float(row[0]))
                btc_id = "BTC"
                high_price_cent = int(float(row[2]) * 100)
                low_price_cent = int(float(row[3]) * 100)
                avg_price = int((high_price_cent + low_price_cent) / 2.0)

                writer.writerow((timestamp, btc_id, avg_price))


if __name__ == "__main__":
    read_and_delete_csv("../data/btcusd_1-min_data.csv", "../data/btc_usd_data.csv")
