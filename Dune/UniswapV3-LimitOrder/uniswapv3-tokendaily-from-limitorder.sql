
with nft_tokens as (
    with call_mint as (
        select 
            output_tokenId as tokenId
            -- from_hex cast string to varbinary
            , from_hex(json_extract_scalar(params_json,'$.token0')) as token0 
            , from_hex(json_extract_scalar(params_json,'$.token1')) as token1 
            -- ! NOTE: Pair_evt_Mint's owner is always NftPositionManager
            -- this recipient is NFT's real owner
            , from_hex(json_extract_scalar(params_json,'$.recipient')) as liquid_provider 
            , cast(json_extract_scalar(params_json,'$.tickLower') as int) as tickLower 
            , cast(json_extract_scalar(params_json,'$.tickUpper') as int) as tickUpper 
            -- call it 'fee_int', remind me, divide it by 1e6 to get real fee rate
            , cast(json_extract_scalar(params_json,'$.fee') as int) as fee_int
        from (
            select 
                m.*,
                json_parse(params) as params_json
            from uniswap_v3_ethereum.NonfungibleTokenPositionManager_call_mint m
            where call_success
                and call_block_time >= now() - interval '{{back_days}}' day
        )
    )
    select 
        cm.*,
        tk0.symbol as symbol0,
        tk1.symbol as symbol1,
        (tk0.symbol || '-' || tk1.symbol) as pair_symbol,
        coalesce(tk0.decimals,18) as tk0decimal,
        coalesce(tk1.decimals,18) as tk1decimal
    from call_mint cm
    inner join tokens.erc20 tk0 
        on cm.token0 = tk0.contract_address
        and tk0.blockchain = 'ethereum'
    inner join tokens.erc20 tk1 
        on cm.token1 = tk1.contract_address
        and tk1.blockchain = 'ethereum'
),
prices as (
    select 
        minute,
        price,
        contract_address
    from prices.usd
    where blockchain = 'ethereum'
        and minute >= now() - interval '{{back_days}}' day
),
add_single_liquidity as (-- add single side liquidity, equivalent as placing a limit order
    select 
        il.evt_block_time as block_time
        -- either amount0 is zero or amount1 is zero
        , (il.amount0 / power(10, nft.tk0decimal) * p0.price + il.amount1 / power(10, nft.tk1decimal) * p1.price) as amt_usd
        --------------------------------
        , case when il.amount0>0 then nft.token0 else nft.token1 end as short_token
        , case when il.amount0>0 then nft.symbol0 else nft.symbol1 end as short_symbol
        , case when il.amount0=0 then nft.token0 else nft.token1 end as long_token
        , case when il.amount0=0 then nft.symbol0 else nft.symbol1 end as long_symbol
        --------------------------------
        , il.liquidity
        , il.tokenId
    from uniswap_v3_ethereum.NonfungibleTokenPositionManager_evt_IncreaseLiquidity il
    inner join nft_tokens nft 
        on il.tokenId = nft.tokenId
    inner join prices p0
        on p0.contract_address = nft.token0
        and p0.minute = date_trunc('minute',il.evt_block_time)
    inner join prices p1
        on p1.contract_address = nft.token1
        and p1.minute = date_trunc('minute',il.evt_block_time)
    where 
        il.evt_block_time >= now() - interval '{{back_days}}' day
        and 
        -- there is some dirty data that both amounts are zero
        ((il.amount0=0 and il.amount1>0) or (il.amount0>0 and il.amount1=0))
),
remove_all_liquidity as (
    -- only care about removing all liquidity placed by limit order previously
    -- this can limit the 'remove liquidity' order to only one previous 'add liquidity' order (most of the time)
    -- not accurate, but simple
    select 
        dl.evt_block_time as block_time,
        asl.short_symbol,
        asl.short_token,
        asl.long_symbol,
        asl.long_token,
        asl.amt_usd,
        dl.liquidity,
        dl.tokenId
    from uniswap_v3_ethereum.NonfungibleTokenPositionManager_evt_DecreaseLiquidity dl 
    join add_single_liquidity asl 
        on asl.tokenId = dl.tokenId
        -- ! NOTE: limit to remove all liquidity, this often find exact one previous 'add liquidity' txn
        -- not accurate, but simple
        and asl.liquidity = dl.liquidity -- close all previous order, exclude partial close, for simplicity
        and dl.evt_block_time > asl.block_time
    where 
        dl.evt_block_time >= now() - interval '{{back_days}}' day
),
token_orders as (
    select 
        block_time,
        short_token as token,
        short_symbol as symbol,
        amt_usd,
        'SHORT' as order_type
    from add_single_liquidity

    union all 

    select 
        block_time,
        long_token as token,
        long_symbol as symbol,
        amt_usd,
        'LONG' as order_type
    from add_single_liquidity

    union all 

    select 
        block_time,
        short_token as token,
        short_symbol as symbol,
        amt_usd,
        'CLOSE_SHORT' as order_type
    from remove_all_liquidity

    union all

    select 
        block_time,
        long_token as token,
        long_symbol as symbol,
        amt_usd,
        'CLOSE_LONG' as order_type
    from remove_all_liquidity
)

select 
    date_trunc('day',block_time) as day,
    token,
    symbol,
    order_type,
    sum(amt_usd) as daily_usd_amt
from token_orders
group by 1,2,3,4
order by day