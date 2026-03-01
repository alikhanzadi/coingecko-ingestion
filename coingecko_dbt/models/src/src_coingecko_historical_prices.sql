WITH raw_crypto_historical_prices AS (
    SELECT
        *
    FROM
        {{ source('crypto_db','raw_crypto_historical_prices') }}
) 
SELECT
    *
FROM raw_crypto_historical_prices