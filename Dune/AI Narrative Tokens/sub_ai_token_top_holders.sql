with one_ai_token as (
    select 
        name as token_name
        , symbol as token_symbol
        , token_address
        , launch_date
        , decimals
    from query_3486591 -- ai token list
    -- have to change to all lower case, because Dune Query a Query cannot accept Upper Case
    where lower(symbol) = lower('{{symbol}}')
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

, current_prices as (
    select 
        p.price 
    from prices.usd_latest p 
    inner join one_ai_token ait-- ai token list
        on p.contract_address = ait.token_address
        and p.symbol = ait.token_symbol
    and p.blockchain = 'ethereum'
)

, holder_balance_percent_usd as (
    select 
        ba.holder
        , ba.balance
        , ba.balance * p.price as balance_usd
        , cast(ba.balance as double)  / ts.total_supply as hold_percent
        , case 
            when cr.address is null then 'EOA'
            else 'Contract'
        end as holder_type
    from holder_balance as ba
    cross join total_supply as ts
    cross join current_prices as p
    left join ethereum.creation_traces as cr 
        on ba.holder = cr.address
)

, holder_balance_rank as (
    select 
        holder 
        , get_href(get_chain_explorer_address('ethereum', holder), holder_type) as link
        , holder_type
        , balance 
        , balance_usd
        , hold_percent
        , rank() over (partition by holder_type order by hold_percent desc) as rank
    from holder_balance_percent_usd
)

select 
    holder
    , link  
    , balance 
    , balance_usd
    , hold_percent
    , sum(hold_percent) over (order by hold_percent desc) as cumsum_hold_percent
from (
    select * from holder_balance_rank
    where holder_type = 'Contract'
        and rank <= 5

    union all

    select * from holder_balance_rank
    where holder_type = 'EOA'
        and rank <= 5
)
order by hold_percent desc

-- select * from "query_3491330(symbol='WLD')"
-- select * from "query_3491330(symbol='INJ')"
-- select * from "query_3491330(symbol='RNDR')"
-- select * from "query_3491330(symbol='VRA')"
-- select * from "query_3491330(symbol='LPT')"
-- select * from "query_3491330(symbol='FET')"
-- select * from "query_3491330(symbol='AGIX')"
