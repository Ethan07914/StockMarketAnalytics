with final as (
select
       ticker,
       date,
       open as open_price,
       high as max_price,
       low as min_price,
       close as close_price,
       volume as shares_traded, --Number of individual shares traded
       vwap as volume_weighted_average_price,
       transactions --Number of transactions (a singular transaction often is for multiple stocks)
from
       {{ ref('seed_stock_info_2026-01-01_2026-01-31') }}
)

select
       *,
       current_timestamp() as ingestion_timestamp
from
       final


