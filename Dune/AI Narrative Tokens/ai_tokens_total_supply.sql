
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
    -- query_3487348: ai token full week series, every week, every token, has one row
    from query_3487348 ws 
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

, weekly_price as (
    select 
        week
        , token_name 
        , token_address
        -- during a token's early days, these tokens have already been minted / burnt / transferred
        -- but during the early history, token's price has NOT been recorded yet
        -- I have to use future price to fill the missing price for the early history
        , coalesce(wp.price, lead(wp.price) ignore nulls over (partition by token_address order by week asc)) as price
    -- query_3487348: ai token full week series, every week, every token, has one row
    from query_3487348 ws
    -- query_3486859: ai token weekly price but miss data for token's early history
    left join query_3486859 as wp
        using (week, token_name, token_address)
)

select 
    week -- columns used in using cannot prefix with table qualifier
    , token_name
    , token_address
    -- +1: small trick, make the column always positive, suitable to display in log-scale
    , ts.total_supply + 1 as total_supply
    , ts.total_supply * wp.price + 1 as total_supply_usd
from total_supply ts
inner join weekly_price as wp -- ai token weekly price
    using (week, token_address, token_name)
order by 1,2