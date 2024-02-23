
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
        sum(delta_liquidity) as range_liquidity
    from mint_burn
    group by 1,2
    having sum(delta_liquidity) > 0
)

select *   
from range_net_liquidity
order by tick_lower