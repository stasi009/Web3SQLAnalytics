-- https://dune.com/queries/3535765
with eth_yield as (
    select 
        block_date
        , daily_yield as eth_yield 
        , total_yield as eth_cumsum_yield
    from query_3535165 -- mq_daily_yield.sql, https://dune.com/queries/3535165
    where yield_currency = 'ETH'
)

, stablecoin_yield as (
    select 
        block_date
        , daily_yield as stablecoin_yield 
        , total_yield as stablecoin_cumsum_yield
    from query_3535165 -- mq_daily_yield.sql, https://dune.com/queries/3535165
    where yield_currency = 'USD'
)

select 
    block_date
    , ey.eth_yield 
    , ey.eth_cumsum_yield
    , sy.stablecoin_yield 
    , sy.stablecoin_cumsum_yield
from eth_yield ey
inner join stablecoin_yield sy
    using (block_date)
order by 1