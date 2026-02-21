select
       id,
       ticker,
       publisher as publisher_name,
       title,
       author as author_name,
       date,
       description,
       sentiment,
       sentiment_reasoning,
       current_timestamp() as ingestion_timestamp
from
       {{ ref('news') }}



