with liquidation as (
    select 
        date_trunc('day',lq.block_timestamp) as day 
        , lq.amount_quote >0 as is_good_asset
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

        , count(distinct iff(is_good_asset,liquidatee,null)) as num_good_liquidatee
        , count(distinct iff(not is_good_asset,liquidatee,null)) as num_bad_liquidatee

        , sum(iff(is_good_asset, amount_quote, null)) as liquidate_good_usd
        , sum(iff(not is_good_asset, amount_quote, null)) as liquidate_bad_usd
    from liquidation
    group by 1
)

, daily_btc_prices as (
    select 
        date_trunc('day',hour) as day 
        , avg(price) as btc_price
    from ARBITRUM.price.ez_hourly_token_prices
    where hour >= current_date - interval '{{back_days}} day' 
        and hour < current_date -- avoid incomplete day
        and token_address = lower('0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f') -- WBTC
    group by 1
)

select 
    l.*  
    , p.btc_price
from daily_liquidation l
right join daily_btc_prices p 
    on l.day = p.day
order by day

