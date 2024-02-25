
with info as (
    select 
        block_timestamp,

        token0_address,
        token0_symbol,
        price_1_0, -- amount of token1 per token0 that the swap, price0 measured in token1
        token0_price, -- token0 price in usd

        token1_address,
        token1_symbol,
        price_0_1, -- amount of token0 per token1 that the swap, price1 measured in token0
        token1_price  -- token1 price in usd
    from ETHEREUM.uniswapv3.ez_swaps
    where block_timestamp >= current_date - interval '1 week'
        and pool_address = lower('{{pool_address}}')
        and token0_price is not null -- the table has some dirty data
        and token1_price is not null -- the table has some dirty data
    order by block_timestamp desc 
    limit 1
)

-- stupid flipside cannot rotate the table when displayed in the dashboard
select 'swap time' as name, block_timestamp as value from info 
union all 

select 'token0 address' as name, token0_address as value from info 
union all 
select 'token0 symbol' as name, token0_symbol as value from info 
union all 
select 'amount of token1 per token0' as name, price_1_0 as value from info 
union all 
select 'token0 usd price' as name, token0_price as value from info 
union all 

select 'token1 address' as name, token1_address as value from info 
union all 
select 'token1 symbol' as name, token1_symbol as value from info 
union all 
select 'amount of token0 per token1' as name, price_0_1 as value from info 
union all 
select 'token1 usd price' as name, token1_price as value from info 