select
       ticker,
       date,
       open as open_price,
       high as max_price,
       low as min_price,
       close as close_price,
       volume as shares_traded,
       --Number of individual shares traded
       vwap as volume_weighted_average_price,
       --Average price of a stock adjusted for the volume of each trade
       transactions, 
       --Number of transactions (a singular transaction often is for multiple stocks)
       current_timestamp() as ingestion_timestamp
from
       {{ source('stocks', 'stock') }}



