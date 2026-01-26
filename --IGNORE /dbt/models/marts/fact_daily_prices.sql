-- dbt/crypto_analytics/models/marts/fact_daily_prices.sql
{{ config(materialized='table', schema='ANALYTICS') }}

SELECT date, 
    coin_id, 
    price, 
    market_cap, 
    total_volume, 
    extraction_timestamp
    
FROM {{ ref('stg_coingecko_prices') }}