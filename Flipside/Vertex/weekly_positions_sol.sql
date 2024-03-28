with weekly_taker_perp_trades as (
    select 
        date_trunc('week',block_timestamp) as week 
        , case 
            WHEN amount < 0 THEN 'short'
            WHEN amount > 0 THEN 'long'
        end as action
        , sum(amount_usd) as amount_usd
    from ARBITRUM.vertex.ez_perp_trades
    where block_timestamp < date_trunc('week',current_date) -- avoid incomplete week
        and is_taker 
        and symbol = 'SOL-PERP'
    group by 1,2
)

, weekly_positions as (
    select 
        week
        , sum(iff(action='long',amount_usd, null)) as long_usd
        , sum(iff(action='short',-amount_usd, null)) as short_usd
    from weekly_taker_perp_trades
    group by 1
)

, weekly_prices as (
    select 
        date_trunc('week',recorded_hour) as week 
        , avg(close) as price
    from solana.price.ez_token_prices_hourly
    where recorded_hour >= date '2023-03-05' -- vertex go online
        and recorded_hour < date_trunc('week',current_date) -- avoid incomplete week
        and token_address = 'So11111111111111111111111111111111111111112' -- Wrapped SOL
    group by 1
)

select 
    week 
    , long_usd
    , short_usd -- negative
    , long_usd + short_usd as "Long over Short"
    , sum(long_usd + short_usd) over (order by week) as open_interest
    , p.price
from weekly_positions s
inner join weekly_prices p
    using (week)



