with stock_metrics as (
select
       stock_metric_pk,
       stock_fk,
       date,
       ticker,
       open_price,
       close_price,
       open_close_difference,
       max_price,
       min_price,
       max_min_difference,
       overnight_return,
       transactions,
       shares_traded,
       volume_weighted_average_price,
       is_new_high,
       is_ten_day_high
from
       {{ ref('int_stock_metrics__calculated') }}
),

article as (
select
       article_sentiment_pk as article_sentiment_fk,
       n.ticker,
       n.date,
from
       {{ ref('stg_polygon__news') }} as n
       inner join {{ ref('article_sentiment_dim') }} as art
       on art.article_id = n.id and art.ticker = n.ticker
),

final as (
select
       sm.stock_metric_pk,
       sm.stock_fk,
       a.article_sentiment_fk,
       d.date_pk as date_fk,
       sm.open_price,
       sm.close_price,
       sm.open_close_difference,
       sm.max_price,
       sm.min_price,
       sm.max_min_difference,
       sm.overnight_return,
       sm.transactions,
       sm.shares_traded,
       sm.volume_weighted_average_price,
       sm.is_new_high,
       sm.is_ten_day_high
from
       stock_metrics as sm
       inner join article as a
       on sm.ticker = a.ticker and sm.date = a.date
       inner join {{ ref('date_dim') }} as d
       on sm.date = d.full_date
)

select
       *,
       current_timestamp() as ingestion_timestamp
from
       final
