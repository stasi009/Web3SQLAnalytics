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
        , iff(t.week = f.first_week, 'New User', 'Old User') as user_type
    from trades t
    inner join user_first_week f
        using (trader)
)

select 
    week
    , user_type 
    , count(distinct trader) as num_traders
from trades_extend_new_old
group by 1,2