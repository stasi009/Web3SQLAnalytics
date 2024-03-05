
--- *************************** total supply
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

, total_supply as (
    select 
        token_name
        , token_address
        , sum(supply) as total_supply
    from (
        select * from mint 
        union all 
        select * from burn
    )
    group by 1,2
)

--- *************************** holder balance
, in_flow as (
    select 
        to as holder
        , token_name
        , token_address
        , value_adjdec
    from query_3486854 as ait -- ai token transfer
    where ait.to <> 0x0000000000000000000000000000000000000000
)

, out_flow as (
    select 
        "from" as holder
        , token_name
        , token_address
        , -1*value_adjdec as value_adjdec
    from query_3486854 as ait -- ai token transfer
    where ait."from" <> 0x0000000000000000000000000000000000000000
)

, holder_balance as (
    select 
        holder 
        , token_name 
        , token_address
        , sum(value_adjdec) as balance
    from (
        select * from in_flow
        union all 
        select * from out_flow
    )
    group by 1,2,3
    having sum(value_adjdec) > 0
)

, holder_balance_percent as (
    select 
        token_name
        , token_address
        , ba.holder
        , ba.balance * 100.0 / ts.total_supply as holder_percent
    from holder_balance as ba
    inner join total_supply as ts
        using (token_name, token_address)
)

--- *************************** MAIN
select 
    token_name 
    , sum(power(holder_percent,2)) as hhi
from holder_balance_percent
group by 1
order by 2 desc
