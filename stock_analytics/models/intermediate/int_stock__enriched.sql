{% set company_dict = {'AAPL': 'Apple', 'GOOG': 'Google', 'MSFT': 'Microsoft', 'AMZN': 'Amazon'} %}

with final as (
select
       distinct
       ticker,
       CASE
       {% for ticker, name in company_dict.items() %}
            WHEN ticker = '{{ ticker }}' THEN '{{ name }}'
       {% endfor %}
       END AS company_name,
       current_timestamp() as ingestion_timestamp
from
       {{ ref('stg_polygon__stock') }}
)

select
       {{ dbt_utils.generate_surrogate_key(['ticker']) }} as stock_pk,
       *
from final
