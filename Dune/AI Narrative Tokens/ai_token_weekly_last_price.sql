with valid_prices as (
    select 
        p.minute
        , date_trunc('week',p.minute) as week
        , ait.token_address
        , ait.name as token_name
        , p.price 
    from prices.usd p
    inner join query_3486591 ait-- ai token list
        on p.contract_address = ait.token_address
        and p.symbol = ait.symbol
        and p.minute >= ait.launch_date
    and p.blockchain = 'ethereum'
)

select 
    week 
    , token_address
    , token_name
    , price
from (
    select 
        week
        , minute
        , token_address
        , token_name
        , price
        , row_number() over (partition by week,token_address order by minute desc) as rn
    from valid_prices
)
where rn = 1
order by 1