with pork_price as (
    select token_price_usd as pork_price_usd
    from dex.prices_latest
    where token_address = 0xb9f599ce614Feb2e1BBe58F180F370D05b39344E
    order by block_time DESC
    limit 1
)
, weth_price as (
    select token_price_usd as weth_price_usd
    from dex.prices_latest
    where token_address = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2
    order by block_time DESC
    limit 1
)
, pork_tick as (
    select 
        ROUND(log(1.0001, pork_price_usd / weth_price_usd) / 200) * 200 as pork_tick,
        power(1.0001, (ROUND(log(1.0001, pork_price_usd / weth_price_usd) / 200) * 200) / 2) as curr_sqrt_price,
        (pork_price_usd / weth_price_usd) * 1e9 as weth_per_billi_pork,
        pork_price_usd,
        weth_price_usd
    from pork_price
       , weth_price
)
, hashes as (
    select distinct evt_tx_hash as hash
    from erc20_ethereum.evt_Transfer xfer
    where contract_address = 0xb9f599ce614Feb2e1BBe58F180F370D05b39344E
      and 0x331399c614cA67DEe86733E5A2FBA40DbB16827c in (xfer."from", xfer."to")
) 
, increase_liquidity as (
    select distinct
        'increase' as liq_type,
        l.evt_tx_hash as hash,
        xfer."to" as address,
        l.tokenId,
        date_trunc('hour', l.evt_block_time) as block_hour,
        l.evt_block_number as block_number,
        l.amount0 / 1e18 as pork,
        l.amount1 / 1e18 as weth,
        l.liquidity / 1e18 as liquidity,
        varbinary_to_int256(log.topic2) as tickLower,
        varbinary_to_int256(log.topic3) as tickHigher
    from uniswap_v3_ethereum.NonfungibleTokenPositionManager_evt_Transfer xfer
    join uniswap_v3_ethereum.NonfungibleTokenPositionManager_evt_IncreaseLiquidity l on l.tokenId = xfer.tokenId
    left join ethereum.logs log on log.tx_hash = xfer.evt_tx_hash 
                           and log.topic0 = 0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde
    join hashes h on h.hash = xfer.evt_tx_hash
)
, decrease_liquidity as (
    select distinct
        'decrease' as liq_type,
        l.evt_tx_hash as hash,
        xfer."to" as address,
        l.tokenId,
        date_trunc('hour', l.evt_block_time) as block_hour,
        l.evt_block_number as block_number,
        (l.amount0 / 1e18) * -1.0 as pork,
        (l.amount1 / 1e18) * -1.0 as weth,
        (l.liquidity / 1e18) * -1.0 as liquidity,
        varbinary_to_int256(log.topic2) as tickLower,
        varbinary_to_int256(log.topic3) as tickHigher
    from uniswap_v3_ethereum.NonfungibleTokenPositionManager_evt_Transfer xfer
    join uniswap_v3_ethereum.NonfungibleTokenPositionManager_evt_DecreaseLiquidity l on l.tokenId = xfer.tokenId
    left join ethereum.logs log on log.tx_hash = xfer.evt_tx_hash 
                           and log.topic0 = 0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c
    join hashes h on h.hash = xfer.evt_tx_hash
)
, liquidity as (
    select * from increase_liquidity 
    union all
    select * from decrease_liquidity
)
, liq_tokens as (
    select tokenId
         , sum(pork) as pork
         , sum(weth) as weth
         , sum(liquidity) as liquidity
         , min(tickLower) as tickLower
         , max(tickHigher) as tickHigher
    from liquidity l
    group by tokenId
)
, tick_ranges AS (
    SELECT
        tokenId,
        tickLower,
        tickHigher,
        liquidity,
        tick,
        power(1.0001, tick) as price_ratio,
        power(1.0001, tick / 2) as sqrt_price_lower,
        power(1.0001, (tick + 200) / 2) as sqrt_price_higher
  FROM liq_tokens
  CROSS JOIN UNNEST(SEQUENCE(cast(tickLower as bigint) + 200, cast(tickHigher as bigint), 200)) AS _u(tick)
)
, tick_liquidity AS (
    SELECT
        tick,
        price_ratio,
        sqrt_price_lower,
        sqrt_price_higher,
        SUM(liquidity) AS liquidity
    FROM tick_ranges
    GROUP BY tick, price_ratio, sqrt_price_lower, sqrt_price_higher
)
, ticks as(
    select *
         , net_liquidity * (sqrt_price_higher - sp) / (sp * sqrt_price_higher) as token0_amount
         , net_liquidity * (sp - sqrt_price_lower) as token1_amount
    from (
        SELECT 
            tick,
            liquidity as net_liquidity,
            price_ratio * 1e9 as weth_per_billi,
            price_ratio * 1e9 * weth_price_usd as usd_per_billi,
            
            price_ratio,
            sqrt_price_lower,
            sqrt_price_higher,
            curr_sqrt_price,
            
            case when 
                case when curr_sqrt_price < sqrt_price_higher then curr_sqrt_price else sqrt_price_higher end > sqrt_price_lower
                then case when curr_sqrt_price < sqrt_price_higher then curr_sqrt_price else sqrt_price_higher end
                else sqrt_price_lower
            end as sp,
            
            case when pork_tick = tick then pork_tick else 0 end as curr_pork_tick,
            case when pork_tick = tick then weth_per_billi_pork else 0 end as curr_weth_per_billi_pork,
            case when pork_tick = tick then weth_per_billi_pork * weth_price_usd else 0 end as curr_usd_per_billi_pork,
            
            pork_tick,
            weth_per_billi_pork,
            weth_per_billi_pork * weth_price_usd as usd_per_billi_pork,
            
            pork_price_usd,
            weth_price_usd
        FROM tick_liquidity
           , pork_tick
    )
)
, mafs as (
    select *
         , token1_amount + (token0_amount * price_ratio) as eth_locked
         , (token1_amount + (token0_amount * price_ratio)) * weth_price_usd as eth_locked_usd
         , sum((token1_amount + (token0_amount * price_ratio))) over (order by tick) as cum_eth
         , sum((token1_amount + (token0_amount * price_ratio)) * weth_price_usd) over (order by tick) as cum_usd
         , ((usd_per_billi - usd_per_billi_pork) / usd_per_billi_pork) as percent_change
         , usd_per_billi / usd_per_billi_pork as x_change
    from ticks
    where tick between pork_tick - {{ticks_lower}} and pork_tick + {{ticks_higher}}
)
, curr_tick as (
    select *
    from mafs
    where curr_weth_per_billi_pork > 0
    limit 1
)
select mafs.*
     
     , mafs.cum_eth - curr_tick.cum_eth as cum_eth_zero
     , case when mafs.cum_eth - curr_tick.cum_eth < 0 then mafs.cum_eth - curr_tick.cum_eth else 0 end as cum_eth_red
     , case when mafs.cum_eth - curr_tick.cum_eth > 0 then mafs.cum_eth - curr_tick.cum_eth else 0 end as cum_eth_green
     
     , mafs.cum_usd - curr_tick.cum_usd as cum_usd_zero
     , case when mafs.cum_usd - curr_tick.cum_usd < 0 then mafs.cum_usd - curr_tick.cum_usd else 0 end as cum_usd_red
     , case when mafs.cum_usd - curr_tick.cum_usd > 0 then mafs.cum_usd - curr_tick.cum_usd else 0 end as cum_usd_green
from mafs
   , curr_tick
order by tick

