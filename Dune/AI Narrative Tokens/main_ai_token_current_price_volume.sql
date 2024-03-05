with latest_price as (
    select 
        ait.name as token_name
        , get_href(get_chain_explorer_address('ethereum', ait.token_address),ait.symbol) as token_url
        , ait.launch_date
        , date_diff('day', ait.launch_date, p.minute) as live_days
        , p.price
        , ait.description
    from query_3486591 ait -- ai token list
    inner join prices.usd_latest p 
        on ait.token_address = p.contract_address
        and ait.symbol = p.symbol
    where p.blockchain = 'ethereum'
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

select 
    token_name -- column used in using, cannot have table qualifier
    , p.token_url 
    , p.launch_date
    , p.live_days 
    , p.price 
    , s.total_supply
    , s.total_supply * p.price as total_supply_usd
    , p.description
from latest_price p
inner join total_supply s 
    using (token_name) -- even single using, also need ()
order by total_supply_usd desc
