{% set company_dict = {
    "NVDA": "NVIDIA Corp",
    "AAPL": "Apple Inc",
    "GOOGL": "Alphabet Inc (Class A)",
    "MSFT": "Microsoft Corp",
    "AMZN": "Amazon.com Inc",
    "META": "Meta Platforms Inc",
    "AVGO": "Broadcom Inc",
    "TSLA": "Tesla Inc",
    "BRK.B": "Berkshire Hathaway Inc (Class B)",
    "WMT": "Walmart Inc",
    "LLY": "Eli Lilly and Co",
    "JPM": "JPMorgan Chase & Co",
    "XOM": "Exxon Mobil Corp",
    "V": "Visa Inc",
    "JNJ": "Johnson & Johnson",
    "MU": "Micron Technology Inc",
    "MA": "Mastercard Inc",
    "ORCL": "Oracle Corp",
    "COST": "Costco Wholesale Corp",
    "ABBV": "AbbVie Inc",
    "BAC": "Bank of America Corp",
    "HD": "Home Depot Inc",
    "PG": "Procter & Gamble Co",
    "CVX": "Chevron Corp",
    "NFLX": "Netflix Inc"
}
%}

with final as (
select
       distinct
       ticker,
       CASE
       {% for ticker, name in company_dict.items() %}
       --Iterate through each ticker in the dictionary
            WHEN ticker = '{{ ticker }}' THEN '{{ name }}'
       --Assign the corresponding company name
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
