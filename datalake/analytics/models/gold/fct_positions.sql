{{ config(
    materialized='table'
) }}

with transactions as (
    select
        client_id,
        split_part(ticker, '-', 1) as ticker,

        sum(
            case
                when transaction_type = 'BUY' then transaction_quantity
                when transaction_type = 'SELL' then -transaction_quantity
            end
        ) as net_quantity,

        sum(
            case
                when
                    transaction_type = 'BUY'
                    then transaction_quantity * transaction_price_brl
                when
                    transaction_type = 'SELL'
                    then -transaction_quantity * transaction_price_brl
            end
        ) as net_invested

    from {{ source('iceberg_curated', 'clients') }}
    group by client_id, ticker
),

--  Preço de ativos (USD)
last_price as (
    select
        symbol,
        price
    from (
        select
            symbol,
            close as price,
            row_number() over (partition by symbol order by datetime desc) as rn
        from {{ source('iceberg_curated', 'stocks') }}
    )
    where rn = 1

    union all

    select
        symbol,
        price
    from (
        select
            symbol,
            close as price,
            row_number() over (partition by symbol order by datetime desc) as rn
        from {{ source('iceberg_curated', 'crypto') }}
    )
    where rn = 1
),

--  Todas moedas → BRL
fx_rates as (
    select
        symbol,
        price_brl
    from (
        select
            symbol,
            sales_cotation as price_brl,
            row_number() over (
                partition by symbol
                order by date_time_cotation desc
            ) as rn
        from {{ source('iceberg_curated', 'coins') }}
    )
    where rn = 1
),

--  USD → BRL (para ativos)
usd_brl as (
    select price_brl as usd_brl
    from fx_rates
    where symbol = 'USD'
)

select
    t.client_id,
    t.ticker,
    t.net_quantity,
    t.net_invested,
    lp.price as current_price_usd,
    usd.usd_brl,
    fx.price_brl as fx_price_brl,
    (t.net_invested / nullif(t.net_quantity, 0)) as avg_price,
    case
        when lp.price is not NULL
            then t.net_quantity * lp.price * usd.usd_brl

        when fx.price_brl is not NULL
            then t.net_quantity * fx.price_brl
    end as position_value_brl,
    case
        when lp.price is not NULL
            then (t.net_quantity * lp.price * usd.usd_brl) - t.net_invested

        when fx.price_brl is not NULL
            then (t.net_quantity * fx.price_brl) - t.net_invested
    end as pnl,
    case
        when lp.price is not NULL
            then
                ((t.net_quantity * lp.price * usd.usd_brl) - t.net_invested)
                / t.net_invested

        when fx.price_brl is not NULL
            then
                ((t.net_quantity * fx.price_brl) - t.net_invested)
                / t.net_invested
    end as return_pct
from transactions as t
left join last_price as lp
    on t.ticker = lp.symbol
left join fx_rates as fx
    on t.ticker = fx.symbol

cross join usd_brl as usd
