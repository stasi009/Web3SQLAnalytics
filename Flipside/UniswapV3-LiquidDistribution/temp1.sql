
----------- sample parameters
-- pool: 0x151CcB92bc1eD5c6D0F9Adb5ceC4763cEb66AC7f
-- created at 2021-05-05
-- fee tier 0.3%, tickSpacing =60
with mint_burn as (
    select 
        tick_lower,
        tick_upper,
        case 
            -- liquidity is misnomer, it's NOT new liquidity after action
            -- it's delta liquidity in that action
            when action = 'INCREASE_LIQUIDITY' then liquidity
            when action = 'DECREASE_LIQUIDITY' then -1*liquidity
        end as delta_liquidity
    from ethereum.uniswapv3.ez_lp_actions
    where pool_address = lower('{{pool_address}}')
        and block_timestamp >= '{{day}}'
),
range_net_liquidity as (
    select 
        tick_lower,
        tick_upper,
        sum(delta_liquidity) as range_liq
    from mint_burn
    group by 1,2
    having sum(delta_liquidity) > 0
),
tick_net_liquidity as (
    select  
        ticks.value as tick,
        sum(range_liq) as tick_net_liq
    from range_net_liquidity as rl, 
        lateral flatten(input => ARRAY_GENERATE_RANGE(tick_lower,tick_upper,{{step}})) as ticks
    group by 1
)

select *   
from tick_net_liquidity
order by tick