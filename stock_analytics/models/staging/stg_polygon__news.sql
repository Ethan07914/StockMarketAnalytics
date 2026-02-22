select
       id,
       ticker,
       publisher as publisher_name,
       title,
       author as author_name,
       date,
       description,
       sentiment,
        --Is the mentioning a specific stock in a positive, neutral or negative way
       sentiment_reasoning,
       --Text to provide context of assigned sentiment
       current_timestamp() as ingestion_timestamp
from
       {{ source('stocks', 'news') }}



