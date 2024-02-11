
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
add_single_liquidity as (
    select 
        il.evt_tx_hash as tx_hash
        , il.evt_block_time as block_time
        , nft.pair_symbol
        , (il.amount0 / power(10, nft.tk0decimal)) as amt0_float
        , (il.amount0 / power(10, nft.tk0decimal) * p0.price) as amt0_usd
        , (il.amount1 / power(10, nft.tk1decimal)) as amt1_float
        , (il.amount1 / power(10, nft.tk1decimal) * p1.price) as amt1_usd
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
)
select * from add_single_liquidity