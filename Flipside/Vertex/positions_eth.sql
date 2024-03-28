with daily_taker_spot_trades as (
    select 
        date_trunc('day',block_timestamp) as day 
        , case 
            WHEN amount < 0 THEN 'sell'
            WHEN amount > 0 THEN 'buy'
        end as action
        , count(distinct trader) as num_takers 
        , sum(amount_usd) as amount_usd
    from ARBITRUM.vertex.ez_spot_trades
    where block_timestamp >= current_date - interval '{{back_days}} day' 
        and block_timestamp < current_date -- avoid incomplete day
        and is_taker 
        and symbol = 'WETH'
    group by 1,2
)
, daily_taker_perp_trades as (
    select 
        date_trunc('day',block_timestamp) as day 
        , case 
            WHEN amount < 0 THEN 'short'
            WHEN amount > 0 THEN 'long'
        end as action
        , count(distinct trader) as num_takers 
        , sum(amount_usd) as amount_usd
    from ARBITRUM.vertex.ez_perp_trades
    where block_timestamp >= current_date - interval '{{back_days}} day' 
        and block_timestamp < current_date -- avoid incomplete day
        and is_taker 
        and symbol = 'ETH-PERP'
    group by 1,2
)

, daily_positions as (
    select 
        day
        , sum(iff(action='long',amount_usd, null)) as long_usd
        , sum(iff(action='short',-amount_usd, null)) as short_usd
        , sum(iff(action='buy',amount_usd, null)) as buy_usd
        , sum(iff(action='sell',-amount_usd, null)) as sell_usd
    from (
        select *  from daily_taker_spot_trades
        union all 
        select *  from daily_taker_perp_trades
    )
    group by 1
)

, daily_prices as (
    select 
        date_trunc('day',hour) as day 
        , avg(price) as price
    from ARBITRUM.price.ez_hourly_token_prices
    where hour >= current_date - interval '{{back_days}} day' 
        and hour < current_date -- avoid incomplete day
        and token_address = lower('0x82aF49447D8a07e3bd95BD0d56f35241523fBab1') -- WETH
    group by 1
)

select 
    day 
    , long_usd
    , short_usd -- negative
    , buy_usd
    , sell_usd -- negative
    , (long_usd + short_usd)/(long_usd - short_usd) as perp_skew
    , (buy_usd + sell_usd)/(buy_usd - sell_usd) as spot_skew
    , p.price
from daily_positions s
inner join daily_prices p
    using (day)


