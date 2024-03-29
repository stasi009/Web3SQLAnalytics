with daily_spot_trades as (
    select 
        date_trunc('day',block_timestamp) as day 
        , iff(amount >0, 'buy', 'sell') as action
        , sum(iff(amount >0, 1, -1) * amount_usd) as volume
    from ARBITRUM.vertex.ez_spot_trades
    where block_timestamp >= current_date - interval '{{back_days}} day'
        and block_timestamp < current_date
        and is_taker
    group by 1,2
)

, daily_perp_trades as (
    select 
        date_trunc('day',block_timestamp) as day 
        , iff(amount >0, 'long', 'short') as action
        , sum(iff(amount >0, 1, -1) * amount_usd) as volume
    from ARBITRUM.vertex.ez_perp_trades
    where block_timestamp >= current_date - interval '{{back_days}} day'
        and block_timestamp < current_date
        and is_taker
    group by 1,2
)

select * from daily_spot_trades
union all 
select * from daily_perp_trades
