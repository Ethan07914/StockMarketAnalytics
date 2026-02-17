import requests
import config
import pandas as pd

base_url = "https://api.polygon.io/v2/reference/news"

class news:
    def __init__(self, url, ticker, interval, date_from, date_to, api_key, order):
        self.url = url
        self.ticker = ticker
        self.interval = interval
        self.date_from = date_from
        self.date_to = date_to
        self.api_key = api_key
        self.order = order

    def set_parameters(self):
        parameters = {
            "ticker": self.ticker,
            "interval": self.interval,
            "date_from": self.date_from,
            "order": self.order,
            "date_to": self.date_to,
            "apiKey": self.api_key
        }
        return parameters

    def get_data(self):
        return requests.get(self.url, self.set_parameters()).json()

    def unnest_insight(self, row):
        subframe = pd.DataFrame(row['insights'])
        row['sentiment'] = subframe['sentiment'].loc[subframe['ticker'] == self.ticker].iloc[0]
        #.iloc[0] prevents the sub data frames index from being included
        row['sentiment_reasoning'] = subframe['sentiment_reasoning'].loc[subframe['ticker'] == self.ticker].iloc[0]
        return row

    def format_data(self):
        data = self.get_data()
        articles = []
        print(data)
        for article in data['results']:
            articles.append(article)
        df = pd.DataFrame(articles)
        df['publisher'] = df.apply(lambda row: row['publisher']['name'], axis=1)
        df['date'] = df['published_utc'].str[:10]
        df['ticker'] = self.ticker
        df = df.apply(self.unnest_insight, axis=1)
        return df[['id', 'ticker', 'publisher', 'title', 'author', 'date', 'description', 'sentiment', 'sentiment_reasoning']]

tickers = ['AAPL', 'GOOG', 'MSFT', 'AMZN']
for ticker in tickers:
    news_data = news(base_url, ticker, 'day', '2026-01-01', '2026-01-31', config.api_key, 'desc')
    if ticker == tickers[0]:
        data = news_data.format_data()
    else:
        data = pd.concat([data, news_data.format_data()])
data = data.reset_index(drop=True)
# data.to_csv(f'seed_stock_news_{news_data.date_from}_{news_data.date_to}.csv', index=False)



