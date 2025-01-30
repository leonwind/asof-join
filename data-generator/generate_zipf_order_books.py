from concurrent.futures import ThreadPoolExecutor
import os

from generate_order_book import *


def generate_single_order_book(
        zipf_skew,
        num_positions,
        num_stocks,
        data_points_per_stock,
        output_dir = None,
        start_time = 0,
        file_suffix = None):

    random_stock_generator = None
    if zipf_skew is None:
        random_stock_generator = UniformStockGenerator(num_stocks)
    else:
        random_stock_generator = ZipfStockGenerator(num_stocks, zipf_skew)

    if output_dir is None:
        output_dir = f"zipf_{str(zipf_skew).replace('.', '_')}" if zipf_skew is not None else "uniform"
    output_path = f"{output_dir}/positions_{num_positions if file_suffix is None else file_suffix}.csv"

    print(f"Start generating {output_path}...", flush=True)
    generate_positions(
        num_stocks,
        data_points_per_stock,
        num_positions,
        random_stock_generator,
        output_path,
        start_time)
    print(f"Finished generating {output_path}\n", flush=True)


def generate_zipf_order_books():
    num_stocks = 70
    data_points_per_stock = 2000000

    zipf_skews = [None, 1.1, 1.3, 1.5, 2]
    increasing_num_positions = [100000 * (2**i) for i in range(0, 8)]

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


def generate_uniform_order_books_different_percentile():
    num_stocks = 128
    data_points_per_stock = 2000000

    end_time_point = data_points_per_stock * StockPrices.SAMPLING_RATE + StockPrices.START_OFFSET

    num_positions = 100000 * (2**6) # Always generate 6_400_000 positions
    percentiles = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.99]

    output_dir = "uniform_percentile"
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    for percentile in percentiles:
        start_time_point = percentile * end_time_point
        generate_single_order_book(
            None, # zipf_skew -> Only generate uniform
            num_positions,
            num_stocks,
            data_points_per_stock,
            output_dir,
            start_time_point,
            file_suffix=f"p{int(percentile * 100)}")


def generate_small_order_books():
    num_stocks = 70
    data_points_per_stock = 2000000
    # Generate from 1, 2, ..., 65536 positions
    increasing_num_positions = [1 * (2**i) for i in range(0, 17)]

    output_dir = "uniform_small"
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    for num_positions in increasing_num_positions:
        generate_single_order_book(
            None,
            num_positions,
            num_stocks,
            data_points_per_stock,
            output_dir)


if __name__ == "__main__":
    #generate_zipf_order_books()
    #generate_small_order_books()
    generate_uniform_order_books_different_percentile()
