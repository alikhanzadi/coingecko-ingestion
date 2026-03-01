{{ config(
    materialized='incremental',
    unique_key=['coin_id', 'date'],
    incremental_strategy='merge',
    cluster_by=['date']
) }}

with source as (

    select
        coin_id,
        date,
        price,
        market_cap,
        total_volume,
        extraction_timestamp
    from {{ source('crypto_db','raw_crypto_historical_prices') }}

),

deduplicated as (

    select *
    from (
        select *,
               row_number() over (
                   partition by coin_id, date
                   order by extraction_timestamp desc
               ) as rn
        from source
    )
    where rn = 1

)

select *
from deduplicated

{% if is_incremental() %}

-- Only process records newer than what's already in target
where extraction_timestamp >
    (select max(extraction_timestamp) from {{ this }})

{% endif %}