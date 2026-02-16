with final as (
select
       ticker,
       publisher as publisher_name,
       title,
       author as author_name,
       date,
       description,
       sentiment,
       sentiment_reasoning
from
       {{ ref('seed_stock_news_2026-01-01_2026-01-31') }}
)

select
       *,
       current_timestamp() as ingestion_timestamp
from
       final