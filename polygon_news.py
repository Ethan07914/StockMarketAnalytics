import requests
import pandas as pd


class news:
    def __init__(self, url, ticker, interval, date_to, api_key, order, limit=1):
        self.url = url
        self.ticker = ticker
        self.interval = interval
        self.date_to = date_to
        self.api_key = api_key
        self.order = order
        self.limit = limit

    def set_parameters(self):
        parameters = {
            "ticker": self.ticker,
            "interval": self.interval,
            "order": self.order,
            "date_to": self.date_to,
            "limit": self.limit,
            "apiKey": self.api_key
        }
        return parameters

    def get_data(self):
        return requests.get(self.url, self.set_parameters()).json() #Retrieve data from api in json format

    def unnest_insight(self, row):
        subframe = pd.DataFrame(row['insights'])
        #Read dictionary format column into a dataframe
        row['sentiment'] = subframe['sentiment'].loc[subframe['ticker'] == self.ticker].iloc[0]
        #Save the sentiment column of the sub dataframe for the rows that are equivalent to the current ticker value to the main dataframe
        row['sentiment_reasoning'] = subframe['sentiment_reasoning'].loc[subframe['ticker'] == self.ticker].iloc[0]
        #Save the associated sentiment reasoning to the sentiment_reasoning column of the main dataframe
        return row

    def format_data(self):
        data = self.get_data()
        articles = []
        #Inialise empty list
        for article in data['results']:
            articles.append(article)
            #List of dictionaries
        df = pd.DataFrame(articles)
        df['publisher'] = df.apply(lambda row: row['publisher']['name'], axis=1)
        #For each row retrieve the nested publisher->name column and create a publisher column to store the value unnested
        df['date'] = self.date_to
        df['ticker'] = self.ticker
        df = df.apply(self.unnest_insight, axis=1)
        return df[['id', 'ticker', 'publisher', 'title', 'author', 'date', 'description', 'sentiment', 'sentiment_reasoning']]





