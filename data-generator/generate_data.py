import sys
import argparse

from generate_price_list import StockPrices
from generate_order_book import *


def generate_data(
        num_stocks, 
        data_per_stock, 
        num_positions, 
        stock_generator: RandomStockGenerator,
        output_prices,
        output_positions):
    stock_prices = StockPrices(num_stocks, data_per_stock)
    stock_prices_df = stock_prices.generate()

    positions = Positions(stock_prices, num_positions, stock_generator)
    positions_df = positions.generate()

    stock_prices_df.to_csv(output_prices, index=False)
    positions_df.to_csv(output_positions, index=False)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_stocks", type=int, required=True)
    parser.add_argument("--num_rows_per_stock", type=int, required=True)
    parser.add_argument("--num_positions", type=int, required=True)
    parser.add_argument("--zipf_skew", type=float, required=False)
    parser.add_argument("--uniform", action="store_true", required=False)
    parser.add_argument("--output_prices", type=str, required=True)
    parser.add_argument("--output_positions", type=str, required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args() 

    use_zipf_distribution = args.zipf_skew is not None
    use_uniform_distribution = args.uniform
    if not use_zipf_distribution and not use_uniform_distribution:
        print("Use either zipf or uniform distribution.")
        sys.exit(1)
    elif use_zipf_distribution and use_uniform_distribution:
        print("Use only one of zipf or uniform distribution.")
        sys.exit(1)

    random_stock_name_generator = None
    if use_zipf_distribution:
        random_stock_name_generator = ZipfStockGenerator(args.num_stocks, args.zipf_skew)
    else:
        random_stock_name_generator = UniformStockGenerator(args.num_stocks)
    
    generate_data(
        args.num_stocks,
        args.num_rows_per_stock,
        args.num_positions,
        random_stock_name_generator,
        args.output_prices,
        args.output_positions)
