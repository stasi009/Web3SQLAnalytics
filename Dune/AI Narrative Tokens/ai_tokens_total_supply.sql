
with display_week_series as (
    with ws as (
        select 
            week
        from unnest(sequence(
            date_trunc('week', date '2020-01-01'),
            date_trunc('week',current_date),
            interval '7' day
        )) as week_tbl(week)
    )
    select 
        ws.week
        , ait.name as token_name
        , ait.symbol
        , ait.token_address
    from query_3486591 ait -- ai token list
    cross join ws
)

, mint as (
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
    with temp as ( -- may have missing
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

    select 
        week
        , token_name 
        , token_address
        , coalesce(week_net_supply,0) as week_net_supply
    from display_week_series ws
    left join temp 
        using (week, token_name, token_address)
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
    week -- columns used in using cannot prefix with table qualifier
    , token_name
    , token_address
    , ts.total_supply
    , ts.total_supply * wp.price as total_supply_usd
from total_supply ts
inner join query_3486859 as wp -- ai token weekly price
    using (week, token_address, token_name)
order by 1,2