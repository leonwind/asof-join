import pandas as pd
from random import randrange


class StockPrices:
    SAMPLING_RATE = 10
    START_OFFSET = 10

    def __init__(self, num_stocks, data_points_per_stock):
        self.stock_names = [f"S-{i}" for i in range(num_stocks)]
        self._data_points_per_stock = data_points_per_stock 

    def _get_random_price(self, min_price=0, max_price=10):
        return randrange(min_price, max_price)

    def _get_time_point(self, curr_row):
        return self.START_OFFSET + self.SAMPLING_RATE * curr_row 

    def get_start_time(self):
        return 0
    
    def get_end_time(self):
        return self._get_time_point(self._data_points_per_stock)
    
    def generate(self):
        stock_data = []
        for i in range(self._data_points_per_stock):
            time_point = self.SAMPLING_RATE * i
            for stock_name in self.stock_names:
                stock_data.append((time_point, stock_name, self._get_random_price()))
        return pd.DataFrame(stock_data, columns=["time", "stock", "price"], dtype="string")
