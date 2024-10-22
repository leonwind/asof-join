import csv


def read_and_delete_csv(source_path, target_path):
    with open(source_path, "r") as source:
        csv_reader = csv.reader(source)
 
        # Skip the header
        next(csv_reader, None)  

        with open(target_path, "w") as target:
            csv_writer = csv.writer(target)

            for row in csv_reader:
                timestamp = int(float(row[0]))
                btc_id = "BTC"
                high_price_cent = int(float(row[2]) * 100)
                low_price_cent = int(float(row[3]) * 100)
                avg_price = int((high_price_cent + low_price_cent) / 2.0)

                csv_writer.writerow((timestamp, btc_id, avg_price))


if __name__ == "__main__":
    read_and_delete_csv("btcusd_1-min_data.csv", "btc_usd_data.csv")

