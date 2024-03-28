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
        and symbol = 'ETH-PERP'
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
        date_trunc('week',hour) as week 
        , avg(price) as price
    from ARBITRUM.price.ez_hourly_token_prices
    where hour >= date '2023-03-05' -- vertex go online
        and hour < date_trunc('week',current_date) -- avoid incomplete week
        and token_address = lower('0x82aF49447D8a07e3bd95BD0d56f35241523fBab1') -- WETH
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

