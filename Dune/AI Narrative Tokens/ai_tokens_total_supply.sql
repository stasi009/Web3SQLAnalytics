
with mint as (
    select 
        block_time
        , token_name
        , token_address
        , value_adjdec as supply
    from query_3486854 as ait -- ai token transfer
    where ait."from" = 0x0000000000000000000000000000000000000000
)

, burn as (
    select 
        block_time
        , token_name
        , token_address
        , -1*value_adjdec as supply
    from query_3486854 as ait -- ai token transfer
    where ait.to = 0x0000000000000000000000000000000000000000
)

, weekly_supply as (
    select 
        date_trunc('week',block_time) as week
        , token_name
        , token_address
        , sum(supply) as week_net_supply
    from (
        select * from mint 
        union all 
        select * from burn
    )
    group by 1,2,3
)

, total_supply as (
    select 
        week 
        , token_name
        , token_address
        , sum(week_net_supply) over (partition by token_address order by week) as total_supply
    from weekly_supply
)

select 
    ts.week
    , ts.token_name
    , ts.token_address
    , ts.total_supply
    , ts.total_supply * wp.price as total_supply_usd
from total_supply ts
inner join query_3486859 as wp -- ai token weekly price
    on ts.week = wp.week 
    and ts.token_address = wp.token_address
    and ts.token_name = wp.token_name
order by 1,2
