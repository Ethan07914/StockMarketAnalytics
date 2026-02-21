select
       ticker,
       date,
       open as open_price,
       high as max_price,
       low as min_price,
       close as close_price,
       volume as shares_traded, --Number of individual shares traded
       vwap as volume_weighted_average_price,
       transactions, --Number of transactions (a singular transaction often is for multiple stocks)
       current_timestamp() as ingestion_timestamp
from
       {{ ref('stock') }}



