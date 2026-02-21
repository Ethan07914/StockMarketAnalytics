select
       stock_pk,
       ticker,
       company_name,
       current_timestamp() as ingestion_timestamp
from
       {{ ref('int_stock__enriched') }}
