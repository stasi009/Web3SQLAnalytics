
-- select 
--     pool_name,
--     block_timestamp,

--     tick_spacing,
--     fee_percent,

--     token0_address,
--     token0_symbol,
--     token0_decimals,

--     token1_address,
--     token1_symbol,
--     token1_decimals,

-- from ethereum.uniswapv3.ez_pools
-- where pool_address = lower('0x151CcB92bc1eD5c6D0F9Adb5ceC4763cEb66AC7f')

select 
    price_1_0, -- amount of token1 per token0 that the swap, price0 calc in token1
    price_0_1, -- amount of token0 per token1 that the swap, price1 calc in token0
    tick,
    amount0_adjusted,
    amount1_adjusted,
    token0_price, -- token0 price in usd
    token1_price, -- token1 price in usd
    amount0_usd,
    amount1_usd,
    '-----------------' as sep,
    -1* amount1_adjusted / amount0_adjusted as my_price_1_0,
    -1* amount0_adjusted / amount1_adjusted as my_price_0_1,
    amount0_usd / amount0_adjusted as my_token0_price_usd,
    amount1_usd / amount1_adjusted as my_token1_price_usd

from ETHEREUM.uniswapv3.ez_swaps
where block_timestamp >= current_date
    and pool_address = lower('0x151CcB92bc1eD5c6D0F9Adb5ceC4763cEb66AC7f')
order by block_timestamp desc 
limit 1