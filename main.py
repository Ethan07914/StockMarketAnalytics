from polygon_news import news
from polygon_stock import stock, client
import config
from config import date, top_25_stocks, url, api_key
import time
import pandas as pd

def fetch_data():
    client1 = client(config).ConfigureClient()
    tickers = list(top_25_stocks.keys())
    loop_count = 0
    for ticker in tickers:
        #Loop through all the stock tickers
        if loop_count != 5:
            #Fetch data for 5 stocks at a time
            loop_count += 1
            #Increment the count
            stock_data = stock(client1, ticker, '1', 'day', date, date)
            #Initialise stock object
            news_data = news(url, ticker, 'day', date, api_key, 'desc')
            #Initialise news object
            if ticker == tickers[0]:
                all_stock_data, all_news_data = stock_data.format_data(), news_data.format_data()
                #Create a dataframe
            else:
                all_stock_data, all_news_data = pd.concat([all_stock_data, stock_data.format_data()]), pd.concat([all_news_data, news_data.format_data()])
                #Append to the dataframe
        else:
            time.sleep(60) #Wait 60 seconds before continuing
            loop_count = 0 #Reset the loop count
    all_stock_data.to_csv('stock_analytics/seeds/stock.csv', index=False) #Store stock data as csv file (seed)
    all_news_data.to_csv('stock_analytics/seeds/news.csv', index=False) #Store news data as csv file (seed)

if __name__ == '__main__':
    fetch_data()
    print('FETCH complete')