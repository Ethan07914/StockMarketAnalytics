{{
config(
       materialized='incremental',
       unique_key='article_sentiment_pk'
      )
}}

{% set sentiments = ['positive', 'negative', 'neutral'] %}

with final as (
select
       {{ dbt_utils.generate_surrogate_key(['id', 'ticker']) }} as article_sentiment_pk,
       id as article_id,
       publisher_name,
       author_name,
       title,
       ticker,
       description,
       sentiment,
       sentiment_reasoning,
       {% for sentiment in sentiments %}
       CASE
            WHEN sentiment = '{{ sentiment }}' THEN 1
            ELSE 0
       END AS is_{{ sentiment }},
       {% endfor %}
       current_timestamp() as ingestion_timestamp
from
       {{ ref('stg_polygon__news') }}
)

select
       *
from
       final

{% if is_incremental() %}

where
      article_sentiment_pk not in (
                        select
                               article_sentiment_pk
                        from
                               {{ this }}
                       )

{% endif %}


