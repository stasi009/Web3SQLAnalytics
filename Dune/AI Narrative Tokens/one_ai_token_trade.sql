
with one_ai_token as (
    select 
        name as token_name
        , symbol as token_symbol
        , token_address
        , launch_date
    from query_3486591 -- ai token list
    where symbol = '{{symbol}}'
)

, trades as (
    select 
        date_trunc('week',block_time) as week
        , td.amount_usd
        , case 
            when td.token_bought_address = ait.token_address then 'buy'
            when td.token_sold_address = ait.token_address then 'sell'
        end as trade_direction
        -- maker is AMM contract, sometimes it's null (e.g., for Uniswap)
        , taker as trader
    from dex.trades td 
    inner join one_ai_token ait 
        on (td.token_bought_address = ait.token_address or td.token_sold_address = ait.token_address)
        and td.block_time >= ait.launch_date
    where td.blockchain = 'ethereum'
)

, valid_prices as (
    select 
        p.minute
        , date_trunc('week',p.minute) as week
        , ait.token_address
        , ait.name as token_name
        , p.price 
    from prices.usd p
    inner join one_ai_token ait-- ai token list
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

select 
    week 
    , sum(amount_usd) as total_volume
    , sum(amount_usd) filter (where trade_direction = 'buy') as buy_volume
    , -1 * sum(amount_usd) filter (where trade_direction = 'sell') as sell_volume
    , approx_distinct(trader) as traders
from trades
group by 1