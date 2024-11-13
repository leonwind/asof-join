import sys
import argparse

from abc import ABC, abstractmethod
import numpy as np
from random import randrange
import pandas as pd

from generate_price_list import StockPrices


class RandomStockGenerator(ABC):
    
    @abstractmethod
    def generate(self):
        pass

class ZipfStockGenerator(RandomStockGenerator):

    def __init__(self, num_stocks, zipf_skew):
        self.num_stocks = num_stocks
        self.zipf_skew = zipf_skew

    def generate(self):
        # Zipf generates in [1, INFINITY), so we do -1 for the index
        idx = np.random.zipf(self.zipf_skew, 1)[0] - 1
        if idx >= self.num_stocks:
            return self.generate()
        return idx


class UniformStockGenerator(RandomStockGenerator):

    def __init__(self, num_stocks):
        self.num_stocks = num_stocks

    def generate(self):
        return randrange(0, self.num_stocks) 


class Positions:
    MIN_AMOUNT = 0
    MAX_AMOUNT = 100

    def __init__(
            self,
            stock_prices: StockPrices,
            num_positions,
            random_stock_generator: RandomStockGenerator):
        self.stock_names = stock_prices.stock_names
        self.num_stocks = len(self.stock_names)
        self.num_positions = num_positions
        self.random_stock_generator = random_stock_generator

        self.start_time = 0
        self.end_time = stock_prices.get_end_time()

    def _get_random_stock(self):
        return self.stock_names[self.random_stock_generator.generate()]
    
    def _get_random_time_point(self):
        return randrange(self.start_time, self.end_time)

    def _get_random_amount(self):
        return randrange(self.MIN_AMOUNT, self.MAX_AMOUNT)

    def generate(self):
        positions = []
        for _ in range(self.num_positions):
            time = self._get_random_time_point()
            stock = self._get_random_stock()
            amount = self._get_random_amount()
            positions.append((time, stock, amount))
        return pd.DataFrame(positions, columns=["time", "stock", "amount"], dtype="string")


def generate_positions(
        num_stocks,
        data_points_per_stock,
        num_positions,
        random_stock_generator,
        output_positions_file,
        shuffle = True):
    prices = StockPrices(num_stocks, data_points_per_stock)
    
    positions = Positions(prices, num_positions, random_stock_generator)
    positions_df = positions.generate()

    if shuffle:
        positions_df = positions_df.sample(frac=1).reset_index(drop=True)

    positions_df.to_csv(output_positions_file, index=False)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_stocks", type=int, required=True)
    parser.add_argument("--num_rows_per_stock", type=int, required=True)
    parser.add_argument("--num_positions", type=int, required=True)
    parser.add_argument("--zipf_skew", type=float, required=False)
    parser.add_argument("--uniform", action="store_true", required=False)
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

    generate_positions(
        args.num_stocks,
        args.num_rows_per_stock,
        args.num_positions,
        random_stock_name_generator,
        args.output_positions)
