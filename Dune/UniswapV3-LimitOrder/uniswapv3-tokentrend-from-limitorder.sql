
/*
! why use 'function call' table and parse the input parameters
other than using uniswap_v3_ethereum.Pair_evt_Mint ??
because Pair_evt_Mint doesn't have tokenId
and Pair_evt_Mint's owner is always NftPositionManager, not the real owner I want
*/
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
add_single_liquidity as (
    select 
        il.evt_tx_hash as tx_hash
        , il.evt_block_time as block_time
        , (nft.symbol0 || '-' || nft.symbol1) as pair_symbol
        --------------------------------
        , nft.symbol0
        , (il.amount0 / power(10, nft.tk0decimal)) as amt0_float
        , (il.amount0 / power(10, nft.tk0decimal) * p0.price) as amt0_usd
        , nft.symbol1
        , (il.amount1 / power(10, nft.tk1decimal)) as amt1_float
        , (il.amount1 / power(10, nft.tk1decimal) * p1.price) as amt1_usd
        --------------------------------
        , case when il.amount0>0 then nft.token0 else nft.token1 end as short_token
        , case when il.amount0>0 then nft.symbol0 else nft.symbol1 end as short_symbol
        , case when il.amount0=0 then nft.token0 else nft.token1 end as long_token
        , case when il.amount0=0 then nft.symbol0 else nft.symbol1 end as long_symbol
        --------------------------------
        , il.liquidity
        , il.tokenId
        , nft.liquid_provider
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
        (il.amount0=0 or il.amount1=0) -- add redundant constraints to reduce result size and speed up
        and il.evt_block_time >= now() - interval '{{back_days}}' day
),
token_short_amount as (
    select 
        short_symbol as tk_symbol,
        short_token as token,
        -- one of amt0_usd and amt1_usd is zero
        sum(amt0_usd + amt1_usd) as short_usd,
        count(tx_hash) as num_short_txn
    from add_single_liquidity
    group by 1,2
),
token_long_amount as (
    select 
        long_symbol as tk_symbol,
        long_token as token,
        -- one of amt0_usd and amt1_usd is zero
        sum(amt0_usd + amt1_usd) as long_usd,
        count(tx_hash) as num_long_txn
    from add_single_liquidity
    group by 1,2
)
select 
    sa.tk_symbol,
    sa.token,
    sa.short_usd,
    sa.num_short_txn,
    la.long_usd,
    la.num_long_txn,
    cast((sa.short_usd - la.long_usd) as double)/(sa.short_usd + la.long_usd) as short_divergence
from token_short_amount sa 
join token_long_amount la
    on sa.tk_symbol = la.tk_symbol
    and sa.token = la.token
where sa.short_usd + la.long_usd >= 100000