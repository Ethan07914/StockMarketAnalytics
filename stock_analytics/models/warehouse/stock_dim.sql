{{
config(
       materialized='incremental',
       unique_key='stock_pk'
      )
}}

select
       stock_pk,
       ticker,
       company_name,
       current_timestamp() as ingestion_timestamp
from
       {{ ref('int_stock__enriched') }}

{% if is_incremental() %}

where
      stock_pk not in (
                        select
                               stock_pk
                        from
                               {{ this }}
                       )

{% endif %}