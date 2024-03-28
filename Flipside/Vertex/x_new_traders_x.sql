
with taker_trades as (
    select 
        date_trunc('week',block_timestamp) as week
        , trader 
        , case 
            when amount<0 then 'short'
            when amount>0 then 'long'
        end as action
        , amount_usd
    from ARBITRUM.vertex.ez_perp_trades
    where block_timestamp < date_trunc('week',current_date) -- avoid incomplete week
        and is_taker 
        and symbol = '{{PerpSymbol}}'
)

-- first trade不再区分方向，我们只关心第一次开仓，不关心第一次平仓
-- , taker_first_days as (
--     select 
--         trader 
--         , min(case when action='long'  then first_day else null end) as first_long_day
--         , min(case when action='short' then first_day else null end) as first_short_day
--     from (
--         select 
--             trader
--             , action
--             , min(day) as first_day
--         from taker_trades
--         group by 1,2
--     )
--     group by 1
-- )

, taker_first_week as (
    select 
        trader 
        , min(week) as first_week
    from taker_trades
    group by 1
)

, extend_trades_with_firstinfo as (
    select 
        tr.week 

        , iff(tr.week = fw.first_week and action='long', trader, null) as trader_first_long 
        , iff(tr.week = fw.first_week and action='long', amount_usd, null) as first_long_vol

        , iff(tr.week = fw.first_week and action='short', trader, null) as trader_first_short
        , iff(tr.week = fw.first_week and action='short', -amount_usd, null) as first_short_vol

    from taker_trades tr
    join taker_first_week fw
        using (trader)
)

select 
    week 

    , count(distinct trader_first_long) as new_longer
    , sum(first_long_vol) as first_long_vol 

    , count(distinct trader_first_short) as new_shorters
    , sum(first_short_vol) as first_short_vol 
from extend_trades_with_firstinfo
group by 1


