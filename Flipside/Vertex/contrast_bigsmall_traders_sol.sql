with taker_trades as (
    select 
        block_timestamp
        , trader
        -- WHEN amount < 0 THEN 'short'
        -- WHEN amount > 0 THEN 'long'
        , iff(amount>0,1,-1) * amount_usd as position_usd
    from ARBITRUM.vertex.ez_perp_trades
    where is_taker 
        and symbol = 'SOL-PERP'
)

, trader_position_scale as (
    select 
        trader 
        , net_position_usd
        , case 
            when net_position_usd >= 500000 then 'long bigwhale'
            when net_position_usd <= -500000 then 'short bigwhale'
            when net_position_usd between -1000 and 1000 then 'retail investor'
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

, weekly_positions_by_scale as (
    select 
        date_trunc('week',tr.block_timestamp) as week
        , scale.trader_scale_level
        , count(distinct trader) as num_traders
        , sum(tr.position_usd) as total_net_position
    from taker_trades tr
    inner join trader_position_scale as scale 
        using (trader)
    where scale.trader_scale_level <> 'medium'
        and tr.block_timestamp < date_trunc('week',current_date) -- avoid incomplete date
    group by 1,2
)

, weekly_num_scales as (--每周有几个scale
    select 
        week
        , count(trader_scale_level) as num_scales  
    from weekly_positions_by_scale
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
    , s.trader_scale_level
    , s.num_traders 
    , s.total_net_position
    -- 不得不做以下处理，否则如果一周有3个scale level，那么相同的price会累加3次再显示
    , p.price / ns.num_scales as price
from weekly_positions_by_scale s
inner join weekly_prices p
    using (week)
inner join weekly_num_scales ns 
    using (week)
order by 1

