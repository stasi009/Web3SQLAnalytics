
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

-- TODO: update following script to make sure I can always get the latest swap even the pool has no swap for a long time
latest_swap as (
    select 
        tick, -- tick after swap
        liquidity, -- liquidity after swap,
        price_1_0 as price0_in1, -- amount of token1 per token0 that the swap, price0 measured in token1
        price_0_1 as price1_in0, -- amount of token0 per token1 that the swap, price1 measured in token0
        token0_price as price0_usd, -- token0 price in usd
        token1_price as price1_usd  -- token1 price in usd
    from ETHEREUM.uniswapv3.ez_swaps
    where block_timestamp >= current_date - interval '1 week'
        and pool_address = lower('{{pool_address}}')
    order by block_timestamp desc 
    limit 1
),

swap_neighbor_lp_temp1 as (

    select 
        tl.tick,
        power(1.0001, tl.tick) * power(10, pi.token0_decimals - pi.token1_decimals) as price0_in1, -- price0 measured by token1
        power(1.0001, tl.tick / 2) as low_sqrtp, -- sqrt price lower bound

        tl.tick + pi.tick_spacing as next_tick,
        power(1.0001, (tl.tick + pi.tick_spacing) / 2) as high_sqrtp, -- sqrt price higher bound

        tl.liquidity,

        sw.tick as now_tick, 
        sw.liquidity as now_liquidity,
        power(1.0001, sw.tick / 2) as now_sqrtp, -- sqrt price after swap

        sw.price0_in1 as now_price0_in1, 
        sw.price1_in0 as now_price1_in0, 
        sw.price0_usd as now_price0_usd, 
        sw.price1_usd as now_price1_usd,

        pi.token0_decimals,
        pi.token1_decimals
    from tick_net_liquidity tl
    cross join latest_swap sw -- just one row
    cross join pool_info pi -- just one row
),

swap_neighbor_lp_temp2 as (
    select 
        tick, -- tick range [tick, next_tick)
        price0_in1,
        1/ price0_in1 as price1_in0,
        liquidity,

        price0_in1 / now_price0_in1 as price0_to_now_ratio,
        1/ price0_in1 / now_price1_in0 as price1_to_now_ratio,

        now_tick,
        now_price0_usd, 
        now_price1_usd,

        -- !NOTE: at tick > current swap tick, all liquidity are composed of token0 
        -- that is because, current tick can only move to those higher ticks when token0 is bought from pool, so higher ticks only need store token0
        case 
            when now_tick < tick then liquidity * (high_sqrtp - low_sqrtp) / (high_sqrtp * low_sqrtp * power(10, token0_decimals)) 
            -- between include both ends
            when now_tick between tick and next_tick then liquidity * (high_sqrtp - now_sqrtp) / (high_sqrtp * now_sqrtp * power(10, token0_decimals)) 
            when now_tick > next_tick then 0
        end as token0_amt_adjdec, -- adjdec: decimals adjusted

        -- !NOTE: at tick < current swap tick, all liquidity are composed of token1
        -- that is because, current tick can only move to those lower ticks when token0 is sold to pool, so lower ticks only need store token1
        case 
            when now_tick < tick then 0
            -- between include both ends
            when now_tick between tick and next_tick then liquidity * (now_sqrtp - low_sqrtp) / power(10, token1_decimals)
            when now_tick > next_tick then liquidity * (high_sqrtp - low_sqrtp) / power(10, token1_decimals)
        end as token1_amt_adjdec -- adjdec: decimals adjusted

    from swap_neighbor_lp_temp1
    where price0_in1 between now_price0_in1 * (1-{{price_delta_ratio}}) and now_price0_in1 * (1+{{price_delta_ratio}})
),

lp_distribution_with_cumsum_liq as (
    select 
        tick, -- tick range [tick, next_tick)
        price0_in1,
        1/ price0_in1 as price1_in0,
        liquidity,

        price0_to_now_ratio,
        price1_to_now_ratio,

        token0_amt_adjdec,
        token1_amt_adjdec,
        (token0_amt_adjdec * now_price0_usd + token1_amt_adjdec * now_price1_usd) as usd_liquidity,
        sum(token0_amt_adjdec * now_price0_usd + token1_amt_adjdec * now_price1_usd) over (order by tick) as cumsum_usd_liq

    from swap_neighbor_lp_temp2
    order by tick
),

zeropoint_cumsum_usdliq as (
    select cumsum_usd_liq as zeropnt_cumsum_usdliq
    from lp_distribution_with_cumsum_liq
    where price0_to_now_ratio <=1
    order by price0_to_now_ratio desc 
    limit 1
),

lp_distribution_with_pressure as (
    select 
        *,
        -1*sell_token0_pressure_usd as buy_token1_pressure_usd, -- become positive
        -1*buy_token0_pressure_usd as sell_token1_pressure_usd -- become negative
    from (
        select 
            lpd.tick, -- tick range [tick, next_tick)
            lpd.price0_in1,
            lpd.price1_in0,
            lpd.liquidity,
            'LP' as flag, -- used in plotting chart

            abs(floor((lpd.price0_to_now_ratio-1) * 100)) as price0_abs_delta_percent,
            abs(floor((lpd.price1_to_now_ratio-1) * 100)) as price1_abs_delta_percent,

            lpd.token0_amt_adjdec,
            lpd.token1_amt_adjdec,
            
            case 
                when cumsum_usd_liq <= zeropnt_cumsum_usdliq then cumsum_usd_liq - zeropnt_cumsum_usdliq -- tick < now_tick
                else 0
            end as sell_token0_pressure_usd, 

            case 
                when cumsum_usd_liq >= zeropnt_cumsum_usdliq then cumsum_usd_liq - zeropnt_cumsum_usdliq -- tick > now_tick
                else 0
            end as buy_token0_pressure_usd
        from lp_distribution_with_cumsum_liq lpd 
        cross join zeropoint_cumsum_usdliq zero
    )
)

select 
    price0_abs_delta_percent,
    sell_token0_pressure_usd,
    buy_token0_pressure_usd,
    -- if sell pressure is higher than buy pressure, encourage to buy, long is strong
    -- the sell pressure stronger, the price is more likely to rise 
    -1*sell_token0_pressure_usd / buy_token0_pressure_usd-1 as long_token0_strength
from (
    select 
        price0_abs_delta_percent,
        -- since group by abs change percent, there is some sell_token0_pressure_usd=0 from buy pressure side
        -- ! NOTE: snowflake doesn't support filter
        avg(case when sell_token0_pressure_usd < 0 then sell_token0_pressure_usd end) as sell_token0_pressure_usd,
        -- since group by abs change percent, there is some buy_token0_pressure_usd=0 from sell pressure side
        avg(case when buy_token0_pressure_usd > 0 then buy_token0_pressure_usd end) as buy_token0_pressure_usd
    from lp_distribution_with_pressure
    group by 1
)
where sell_token0_pressure_usd is not null 
    and buy_token0_pressure_usd is not null
order by 1 