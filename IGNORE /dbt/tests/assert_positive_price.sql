-- dbt/crypto_analytics/tests/assert_positive_price.sql
SELECT coin_id, date, price
FROM {{ ref('fact_daily_prices') }}
WHERE price <= 0