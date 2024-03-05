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
        , tsf.contract_address as token_address
        , ait.name as token_name
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
    select 
        block_time
        , token_name
        , token_address
        , value_adjdec as supply
    from aitoken_token_transfer as ait -- ai token transfer
    where ait."from" = 0x0000000000000000000000000000000000000000
)

, burn as (
    select 
        block_time
        , token_name
        , token_address
        , -1*value_adjdec as supply
    from aitoken_token_transfer as ait -- ai token transfer
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
    from aitoken_token_transfer as ait -- ai token transfer
    where ait.to <> 0x0000000000000000000000000000000000000000
)

, out_flow as (
    select 
        "from" as holder
        , token_name
        , token_address
        , -1*value_adjdec as value_adjdec
    from aitoken_token_transfer as ait -- ai token transfer
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
    left join non_wallet_contracts nwc -- contracts exclude wallets and safe
        on nwc.address = ba.holder
    where nwc.address is null -- holder cannot be contract (except wallet or DAO)
)