
-- sample pool: 0x151CcB92bc1eD5c6D0F9Adb5ceC4763cEb66AC7f
with pool_info as (
    select 
        pool_address,
        pool_name,
        block_timestamp as pool_create_time,

        tick_spacing,
        fee_percent,

        token0_address,
        token0_symbol,
        token0_decimals,

        token1_address,
        token1_symbol,
        token1_decimals

    from ethereum.uniswapv3.ez_pools
    where pool_address = lower('{{pool_address}}')
    limit 1 -- can only match exactly one pool
),

mint_burn as (
    select 
        lp.tick_lower,
        lp.tick_upper,
        pinfo.tick_spacing,
        case 
            -- liquidity is misnomer, it's NOT new liquidity after action
            -- it's delta liquidity in that action
            when lp.action = 'INCREASE_LIQUIDITY' then lp.liquidity
            when lp.action = 'DECREASE_LIQUIDITY' then -1*lp.liquidity
        end as delta_liquidity
    from ethereum.uniswapv3.ez_lp_actions lp
    inner join pool_info pinfo
        on lp.pool_address = pinfo.pool_address
        and lp.block_timestamp >= pinfo.pool_create_time
),
range_net_liquidity as (
    select 
        tick_lower,
        tick_upper,
        tick_spacing,
        sum(delta_liquidity) as range_liq
    from mint_burn
    group by 1,2,3
    having sum(delta_liquidity) > 0
),

tick_net_liquidity as (
    select  
        ticks.value as tick,
        sum(range_liq) as liquidity
    from range_net_liquidity as rl, 
        lateral flatten(input => ARRAY_GENERATE_RANGE(tick_lower,tick_upper,tick_spacing)) as ticks
    group by 1
),

latest_swap as (
    select 
        tick, -- tick after swap
        liquidity, -- liquidity after swap,
        'SW' as flag -- used in plot chart
        -- price_1_0 as price0_in1, -- amount of token1 per token0 that the swap, price0 calc in token1
        -- price_0_1 as price1_in0, -- amount of token0 per token1 that the swap, price1 calc in token0
        -- token0_price as price0_usd, -- token0 price in usd
        -- token1_price as price1_usd  -- token1 price in usd
    from ETHEREUM.uniswapv3.ez_swaps
    where block_timestamp >= current_date
        and pool_address = lower('{{pool_address}}')
    order by block_timestamp desc 
    limit 1
),

swap_neighbor_liquidity as (
    select 
        tl.tick,
        tl.liquidity,
        'LP' as flag
    from tick_net_liquidity tl
    cross join latest_swap sw
    where tl.tick between sw.tick - 10000 and sw.tick + 10000
)

select * from swap_neighbor_liquidity
union all
select * from latest_swap