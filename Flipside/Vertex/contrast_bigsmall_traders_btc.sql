with taker_trades as (
    select 
        block_timestamp
        , trader
        -- WHEN amount < 0 THEN 'short'
        -- WHEN amount > 0 THEN 'long'
        , iff(amount>0,1,-1) * amount_usd as position_usd
    from ARBITRUM.vertex.ez_perp_trades
    where is_taker 
        and symbol = 'BTC-PERP'
)

, trader_position_scale as (
    select 
        trader 
        , net_position_usd
        , case 
            when net_position_usd >= 500000 then 'long bigwhale'
            when net_position_usd <= -500000 then 'short bigwhale'
            when net_position_usd between 0 and 1000 then 'long retail'
            when net_position_usd between -1000 and 0 then 'short retail'
            else 'medium'
        end as trader_scale_level
    from (
        select 
            trader
            , sum(position_usd) as net_position_usd
        from taker_trades
        group by 1
    )
)

, stats_per_scale_level as (
    select 
        trader_scale_level
        , count(trader) as num_traders
        , sum(net_position_usd) as total_position_usd
    from trader_position_scale
    group by 1
)

select 
    date_trunc('week',tr.block_timestamp) as week
    , scale.trader_scale_level
    , count(distinct trader) as num_traders
    , sum(tr.position_usd) as total_position
from taker_trades tr
inner join trader_position_scale scale
    using(trader)
where scale.trader_scale_level <> 'medium'
    and tr.block_timestamp < date_trunc('week',current_date) -- avoid incomplete date
group by 1,2
