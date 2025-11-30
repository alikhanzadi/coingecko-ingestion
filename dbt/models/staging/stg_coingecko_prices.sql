-- dbt/crypto_analytics/models/staging/stg_coingecko_prices.sql
{{ config(materialized='view', schema='RAW') }}

SELECT coin_id, 
    date, 
    price, 
    market_cap, 
    total_volume, 
    extraction_timestamp
    
FROM {{ source('coingecko', 'COINGECKO_HISTORICAL_PRICES') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY coin_id, date ORDER BY extraction_timestamp DESC) = 1