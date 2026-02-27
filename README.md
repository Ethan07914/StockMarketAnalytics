# Stock Market Analytics

## Introduction

This project is an automated end-to-end data pipeline designed to track and analyze stock market performance alongside media sentiment.
By orchestrating the dataflow from the Polygon API through a dbt-transformed BigQuery warehouse, the system turns raw financial data into an actionable Star Schema.
The infrastructure is fully containerized with Docker and deployed via Google Cloud Run, bringing in new data without human intervention. 

## Dataflow

- Python script makes request to Polygon API for stock and related news data.
- Python Classes and Pandas Library used to format data.
- Data written to CSV files.
- Staging, Intermediate and Warehouse dbt models (Incremental) used to transform data.
- Data is written to Star Schema Data Warehouse on BigQuery.
- Entire data pipeline encapsulated in a Docker Image.
- Docker Image pushed to Google Cloud Artifact Registry.
- Docker Image automated with Cloud Run and Cloud Schedule to run five times a week (after trading days).
- Looker studio dashboard with Article Sentiment and Stock Metric analysis.
 
![DataFlow.png](DataFlow.png)

## Tech Stack

- Python (OOP)
- dbt-core & dbt-bigquery
- Docker
- BigQuery
- Pandas
- Google Cloud Run & Scheduler
- Looker Studio

## dbt Lineage Graph

- Stock.csv and News.csv are seeded 
- Staging models used to centralise data and invoke consistent naming conventions
- Intermediate models handle complex calculation, business logic and surrogate key generation
- Warehouse models present data in a user-friendly way

![img_3.png](img_3.png)

## Physical Data Model

- Data is modelled into a Star Schema
- Central stock_metric_fct fact table which hold key metrics
- article_sentiment_dim, date_dim and stock_dim dimension tables provide context to metrics 


![img_4.png](img_4.png)

## Dashboard

- Dashboard created with Looker Studio powered by BigQuery dataset
- Link: https://lookerstudio.google.com/reporting/fe8bbbde-0b35-4c62-bb29-014a29fb03fe

![img_5.png](img_5.png)

![img_6.png](img_6.png)

## Improvements

- Utilisation of a semantic layer BI tool such as Looker
- Bring in more data at an increased frequency for closer to real time analytics






