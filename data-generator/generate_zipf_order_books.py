from concurrent.futures import ThreadPoolExecutor
import os

from generate_order_book import *


def generate_single_order_book(
        zipf_skew,
        num_positions,
        num_stocks,
        data_points_per_stock):
    random_stock_generator = None
    if zipf_skew is None:
        random_stock_generator = UniformStockGenerator(num_stocks)
    else:
        random_stock_generator = ZipfStockGenerator(num_stocks, zipf_skew)

    output_dir = f"zipf_{str(zipf_skew).replace('.', '_')}" if zipf_skew is not None else "uniform"
    output_path = f"{output_dir}/positions_{num_positions}.csv"

    #generate_positions(
    #    num_stocks,
    #    data_points_per_stock,
    #    num_positions,
    #    random_stock_generator,
    #    output_path)

    print(f"Finished generating {output_path}", flush=True)


def generate_zipf_order_books():
    num_stocks = 70
    data_points_per_stock = 2000000

    zipf_skews = [None, 1.1, 1.3, 1.5, 2]
    increasing_num_positions = [100000 * (2**i) for i in range(0, 8)]
    #increasing_num_positions = [1 * (2**i) for i in range(0, 8)]

    tasks = []
    for zipf_skew in zipf_skews:
        output_dir = f"zipf_{str(zipf_skew).replace('.', '_')}" if zipf_skew is not None else "uniform" 

        if not os.path.exists(output_dir):
            os.mkdir(output_dir)

        for num_positions in increasing_num_positions:
            tasks.append((zipf_skew, num_positions))

    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(generate_single_order_book, 
                zipf_skew,
                num_positions,
                num_stocks,
                data_points_per_stock)
            for zipf_skew, num_positions in tasks]

        for future in futures:
            future.result() 


if __name__ == "__main__":
    generate_zipf_order_books()
