{{
config(
       materialized='incremental',
       unique_key='stock_metric_pk'
      )
}}

with stock as (
select
        s.date,
        s.ticker,
        se.stock_pk as stock_fk,
        s.open_price,
        s.close_price,
        (s.close_price - s.open_price) as open_close_difference,
        s.max_price,
        s.min_price,
        (s.max_price - s.min_price) as max_min_difference,
        transactions,
        shares_traded,
        volume_weighted_average_price
from
        {{ ref('stg_polygon__stock') }} as s
        inner join {{ ref('int_stock__enriched') }} as se
        on s.ticker = se.ticker
),

highest_price as (
select
       ticker,
       MAX(close_price) as all_time_max_price
       --Find the highest all time price by stock
from
       {{ ref('stg_polygon__stock') }}
group by
       ticker
),

ten_day_price_high as (
select
       ticker,
       date,
       close_price,
       CASE
            WHEN s.close_price = MAX(s.close_price)
                 OVER (partition by ticker order by date
                 rows between 9 preceding and current row)
                 THEN 1
                 --Is the previous stock close price the highest it has been in the last 10 days
             ELSE 0
       END AS ten_day_high
from
       {{ ref('stg_polygon__stock') }} as s
),

overnight_price_change as (
select
       ticker,
       date,
       open_price - lag(close_price) over(partition by ticker order by date) as overnight_return
       -- Subtract the close price from the previous day to the close price today
from
       {{ ref('stg_polygon__stock') }} as s
order by ticker, date
),

final as (
select
        {{dbt_utils.generate_surrogate_key(['s.ticker', 's.date'])}} as stock_metric_pk,
        s.ticker,
        s.date,
        s.stock_fk,
        s.open_price,
        s.close_price,
        s.open_close_difference,
        s.max_price,
        s.min_price,
        s.max_min_difference,
        s.transactions,
        s.shares_traded,
        s.volume_weighted_average_price,
        CASE
             WHEN hp.all_time_max_price <= s.close_price THEN 1
             ELSE 0
        END AS is_new_high,
        COALESCE(tdph.ten_day_high,1) as is_ten_day_high,
        COALESCE(opc.overnight_return, 0) as overnight_return
        --Supplement NULL values with zero if stocks where not traded the previous day
from
        stock as s
        left join highest_price as hp
        on s.ticker = hp.ticker
        left join ten_day_price_high as tdph
        on s.ticker = tdph.ticker and s.date = tdph.date
        left join overnight_price_change as opc
        on s.ticker = opc.ticker and s.date = opc.date
)

select
       *,
       current_timestamp() as ingestion_timestamp
from final

{% if is_incremental() %}

where
      stock_metric_pk not in (
                        select
                               stock_metric_pk
                        from
                               {{ this }}
                       )

{% endif %}

