
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
        pc.pool,
        tk0.symbol as symbol0,
        tk1.symbol as symbol1,
        (tk0.symbol || '-' || tk1.symbol || '-' || cast(pc.fee as varchar)) as pair_symbol,
        coalesce(tk0.decimals,18) as tk0decimal,
        coalesce(tk1.decimals,18) as tk1decimal
    from call_mint cm
    inner join tokens.erc20 tk0 
        on cm.token0 = tk0.contract_address
        and tk0.blockchain = 'ethereum'
    inner join tokens.erc20 tk1 
        on cm.token1 = tk1.contract_address
        and tk1.blockchain = 'ethereum'
    inner join uniswap_v3_ethereum.Factory_evt_PoolCreated pc
        on pc.token0 = cm.token0 
        and pc.token1 = cm.token1
        and pc.fee = cm.fee_int -- same token pair can have multiple fee tiers
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
add_liquidity as (
    select 
        il.evt_tx_hash as tx_hash
        , il.evt_block_time as block_time
        , nft.pool
        , nft.pair_symbol
        --------------------------------
        , nft.symbol0
        , (il.amount0 / power(10, nft.tk0decimal)) as amt0_float
        , (il.amount0 / power(10, nft.tk0decimal) * p0.price) as amt0_usd
        , nft.symbol1
        , (il.amount1 / power(10, nft.tk1decimal)) as amt1_float
        , (il.amount1 / power(10, nft.tk1decimal) * p1.price) as amt1_usd
        --------------------------------
        , case 
            when il.amount0 > 0 and il.amount1 > 0 then 'Neutral' -- provide in-range liquidity
            when il.amount0 > 0 and il.amount1 = 0 then 'S0L1' -- short 0 long 1
            when il.amount0 = 0 and il.amount1 > 0 then 'S1L0' -- short 1 long 0
            else null -- impossible
            end as lp_intention 
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
        il.evt_block_time >= now() - interval '{{back_days}}' day
),
pair_add_liquidity_stats as (
    select 
        pool,
        pair_symbol,
        
        sum(intention_usd) as Total_USD,
        sum(case when lp_intention = 'Neutral' then intention_usd else 0 end) as Neutral_USD,
        sum(case when lp_intention = 'S0L1' then intention_usd else 0 end) as S0L1_USD,
        sum(case when lp_intention = 'S1L0' then intention_usd else 0 end) as S1L0_USD,

        sum(num_txn) as Total_Txns,
        sum(case when lp_intention = 'Neutral' then num_txn else 0 end) as Neutral_Txns,
        sum(case when lp_intention = 'S0L1' then num_txn else 0 end) as S0L1_Txns,
        sum(case when lp_intention = 'S1L0' then num_txn else 0 end) as S1L0_Txns
    from (
        select  
            pool,
            pair_symbol,
            lp_intention,
            sum(amt0_usd + amt1_usd) as intention_usd,
            count(tx_hash) as num_txn
        from add_liquidity
        group by 1,2,3
    )
        group by 1,2
)

select 
    get_href(get_chain_explorer_address('ethereum', pool),pair_symbol) as token_pair,
    ------------------------- USD distribution
    Neutral_USD,
    S0L1_USD,
    S1L0_USD,
    (Neutral_USD / Total_USD) as Neutral_USD_Percent,
    (S0L1_USD / Total_USD) as S0L1_USD_Percent,
    (S1L0_USD / Total_USD) as S1L0__USD_Percent,
    ------------------------- Num Txn distribution
    Neutral_Txns,
    S0L1_Txns,
    S1L0_Txns
from pair_add_liquidity_stats 
where Total_USD >= 100000
order by Total_USD desc