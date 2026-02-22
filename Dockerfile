FROM python:3.11-slim

WORKDIR /StockMarketPredictor

COPY . .

RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN pip install dbt-bigquery

CMD ["sh",
     "-c",
     "python main.py \
     && dbt seed --project-dir ./stock_analytics --profiles-dir ./stock_analytics \
     && dbt run --project-dir ./stock_analytics --profiles-dir ./stock_analytics \
     && dbt test --project-dir ./stock_analytics --profiles-dir ./stock_analytics"]