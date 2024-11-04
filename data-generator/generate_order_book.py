from abc import ABC, abstractmethod
from random import randrange
import numpy as np
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
    def __init__(self, stock_prices: StockPrices, num_positions, random_stock_generator: RandomStockGenerator):
        self.stock_names = stock_prices.stock_names
        self.num_stocks = len(self.stock_names)
        self.num_positions = num_positions
        self.random_stock_generator = random_stock_generator

        self.start_time = 0
        self.end_time = stock_prices.get_end_time()

        self.min_amount = 1
        self.max_amount = 10

    def _get_random_stock(self):
        return self.stock_names[self.random_stock_generator.generate()]
    
    def _get_random_time_point(self):
        return randrange(self.start_time, self.end_time)

    def _get_random_amount(self):
        return randrange(self.min_amount, self.max_amount)

    def generate(self):
        positions = []
        for _ in range(self.num_positions):
            time = self._get_random_time_point()
            stock = self._get_random_stock()
            amount = self._get_random_amount()
            positions.append((time, stock, amount))
        return pd.DataFrame(positions, columns=["time", "stock", "amount"], dtype="string")
