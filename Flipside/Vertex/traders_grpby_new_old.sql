with trades as (
    select 
        date_trunc('week',block_timestamp) as week 
        , trader
    from ARBITRUM.vertex.ez_spot_trades
    where is_taker

    union all 

    select 
        date_trunc('week',block_timestamp) as week 
        , trader
    from ARBITRUM.vertex.ez_perp_trades
    where is_taker
)

, user_first_week as (
    select 
        trader 
        , min(week) as first_week 
    from trades
    group by 1
)

, trades_extend_new_old as (
    select 
        trader 
        , t.week 
        , t.week = f.first_week is_new_user
    from trades t
    inner join user_first_week f
        using (trader)
)

select 
    *
    , cast(num_new_traders as double)/(num_new_traders+num_old_traders) as new_trader_ratio
from (
    select 
        week
        , count(distinct iff(is_new_user,trader,null)) as num_new_traders
        , count(distinct iff(not is_new_user,trader,null)) as num_old_traders
    from trades_extend_new_old
    group by 1
)

