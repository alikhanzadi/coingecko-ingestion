-- dbt/crypto_analytics/models/marts/dim_coins.sql
{{ config(materialized='table', schema='ANALYTICS') }}

SELECT DISTINCT coin_id, -- Add more descriptive fields if you were enriching with other APIs (e.g., coin name, symbol) CURRENT_TIMESTAMP() as created_at
FROM {{ ref('stg_coingecko_prices') }}