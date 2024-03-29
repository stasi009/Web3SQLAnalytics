
with daily_spot_trades as (
    select 
        date_trunc('day',block_timestamp) as day 
        , case 
            when symbol = 'WBTC' then 'BTC'
            when symbol = 'WETH' then 'ETH'
            else symbol
        end as asset_type
        , amount_usd
    from ARBITRUM.vertex.ez_spot_trades
    where block_timestamp >= current_date - interval '{{back_days}} day'
        and block_timestamp < current_date
        and is_taker
)

, daily_perp_trades as (
    select 
        date_trunc('day',block_timestamp) as day 
        , split(symbol,'-')[0] as asset_type
        , amount_usd
    from ARBITRUM.vertex.ez_perp_trades
    where block_timestamp >= current_date - interval '{{back_days}} day'
        and block_timestamp < current_date
        and is_taker
)

select 
    day 
    , asset_type
    , sum(amount_usd) as volume
from (
    select * from daily_spot_trades
    union all 
    select * from daily_perp_trades
)
group by 1,2
order by 1,2

