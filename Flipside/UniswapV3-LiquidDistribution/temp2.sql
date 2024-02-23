
select 
    pool_name,
    block_timestamp,

    tick_spacing,
    fee_percent,

    token0_address,
    token0_symbol,
    token0_decimals,

    token1_address,
    token1_symbol,
    token1_decimals,

from ethereum.uniswapv3.ez_pools
where pool_address = lower('0x151CcB92bc1eD5c6D0F9Adb5ceC4763cEb66AC7f')