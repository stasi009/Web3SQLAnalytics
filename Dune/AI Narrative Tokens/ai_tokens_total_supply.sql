
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

, week_last_price as (

)

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
order by 1,2,3
