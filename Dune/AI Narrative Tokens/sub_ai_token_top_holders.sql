with one_ai_token as (
    select 
        name as token_name
        , symbol as token_symbol
        , token_address
        , launch_date
    from query_3486591 -- ai token list
    where symbol = '{{symbol}}'
)

, aitoken_token_transfer as (
    select
        tsf.evt_block_time as block_time
        , tsf."from"
        , tsf.to
        , tsf.value / power(10, ait.decimals) as value_adjdec -- adjdec means "decimals adjusted"
    from erc20_ethereum.evt_Transfer tsf 
    inner join one_ai_token ait -- ai token list
        on tsf.contract_address = ait.token_address -- only care about ai tokens
        and tsf.evt_block_time >= ait.launch_date -- redundant condition, but can speed up to add constraints on time
)

--- *************************** total supply
, mint as (
    select value_adjdec as supply
    from aitoken_token_transfer as ait -- ai token transfer
    where ait."from" = 0x0000000000000000000000000000000000000000
)

, burn as (
    select -1*value_adjdec as supply
    from aitoken_token_transfer as ait -- ai token transfer
    where ait.to = 0x0000000000000000000000000000000000000000
)

, total_supply as (
    select sum(supply) as total_supply
    from (
        select * from mint 
        union all 
        select * from burn
    )
)

--- *************************** holder balance
, in_flow as (
    select 
        to as holder
        , value_adjdec
    from aitoken_token_transfer as ait -- ai token transfer
    where ait.to <> 0x0000000000000000000000000000000000000000
)

, out_flow as (
    select 
        "from" as holder
        , -1*value_adjdec as value_adjdec
    from aitoken_token_transfer as ait -- ai token transfer
    where ait."from" <> 0x0000000000000000000000000000000000000000
)

, holder_balance as (
    select 
        holder 
        , sum(value_adjdec) as balance
    from (
        select * from in_flow
        union all 
        select * from out_flow
    )
    group by 1
    having sum(value_adjdec) > 0
)

, holder_balance_percent as (
    select 
        ba.holder
        , ba.balance * 100.0 / ts.total_supply as holder_percent
    from holder_balance as ba
    cross join total_supply as ts
)