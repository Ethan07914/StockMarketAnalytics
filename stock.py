from massive import RESTClient
import config
from urllib3 import HTTPResponse
from typing import cast
import pandas as pd

class client:
    def __init__(self, config_file_name):
        self.config_file_name = config_file_name

    def ConfigureClient(self):
        return RESTClient(self.config_file_name.api_key)

class stock:
    def __init__(self, client, ticker, multiplier, interval, date_from, date_to, raw=True):
        self.client = client
        self.ticker = ticker
        self.multiplier = multiplier
        self.interval = interval
        self.date_from = date_from
        self.date_to = date_to
        self.raw = raw

    def get_data(self):
        aggs = cast(
            HTTPResponse,
            self.client.get_aggs(
                self.ticker,
                self.multiplier,
                self.interval,
                self.date_from,
                self.date_to,
                self.raw
            ))
        return aggs

    def format_data(self):
        df = pd.DataFrame(self.get_data())
        df['ticker'] = self.ticker
        df['date'] = pd.to_datetime(df['timestamp'], unit='ms').dt.date
        df['volume'] = df['volume'].astype(int)
        return df[['ticker', 'date', 'open', 'high', 'low', 'close', 'volume', 'vwap', 'timestamp', 'transactions']]
        #VWAP (Volume Weighted Average Price)

client = client(config).ConfigureClient()
tickers = ['AAPL', 'GOOG', 'MSFT', 'AMZN']
for ticker in tickers:
    stock_data = stock(client, ticker, '1', 'day', '2026-01-01', '2026-01-31')
    if ticker == tickers[0]:
        data = stock_data.format_data()
    else:
        data = pd.concat([data, stock_data.format_data()])
data.to_csv(f'stock_info_{stock_data.date_from}_{stock_data.date_to}.csv', index=False)


