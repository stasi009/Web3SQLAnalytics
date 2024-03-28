
with taker_trades as (
    select 
        date_trunc('day',block_timestamp) as day
        , trader 
        , case 
            when amount<0 then 'short'
            when amount>0 then 'long'
        end as action
        , amount_usd
    from ARBITRUM.vertex.ez_perp_trades
    where is_taker 
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

, taker_first_day as (
    select 
        trader 
        , min(day) as first_day
    from taker_trades
    group by 1
)

select 
    tr.day 
    , trader

    , iff(tr.day = fd.first_day and action='long', 1, 0) as first_long 
    , iff(tr.day = fd.first_day and action='long', amount_usd, null) as first_long_vol

    , iff(tr.day = fd.first_day and action='short', 1, 0) as first_short
    , iff(tr.day = fd.first_day and action='short', -amount_usd, null) as first_short_vol

from taker_trades tr
join taker_first_day fd
    using (trader)

