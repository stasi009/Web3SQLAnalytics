
with liquidation as (
    select 
        date_trunc('day',lq.block_timestamp) as day 
        , case 
            when lq.amount_quote >0 then 'Good'
            else 'Bad' 
        end as liq_asset_quality
        , lq.health_group_symbol as liq_asset_token
        , lq.amount_quote -- unit: usd
        , trader as liquidatee
    from ARBITRUM.vertex.ez_liquidations lq 
    where block_timestamp >= current_date - interval '{{back_days}} day' 
        and block_timestamp < current_date -- avoid incomplete day
)

, daily_liquidation as (
    select 
        day
        , liq_asset_quality as "Liquidate Asset Quality"
        , count(distinct liquidatee) as "Daily Liquidatee"
        , sum(amount_quote) as "Daily Liquidate USD"
    from liquidation
    group by 1,2
)

, daily_btc_prices as (
    select 
        date_trunc('day',hour) as day 
        , avg(price) as daily_btc_price
    from ARBITRUM.price.ez_hourly_token_prices
    where hour >= current_date - interval '{{back_days}} day' 
        and hour < current_date -- avoid incomplete day
        and token_address = lower('0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f') -- WBTC
    group by 1
)

select * 
from daily_liquidation
order by day, "Liquidate Asset Quality"

