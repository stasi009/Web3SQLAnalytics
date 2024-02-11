
with pair_add_liquidity as (
    -- aim of this CTE is to find pair information
    select
        em.evt_tx_hash, 
        -- owner is useless, most of the time, owner is NftPositionManager, it's not the real owner
        em.contract_address as pair_addr,
        pc.token0,
        tk0.symbol as tk0symbol,
        (em.amount0 / power(10, tk0.decimals)) as amt0_float,
        pc.token1,
        tk1.symbol as tk1symbol,
        (em.amount1 / power(10, tk1.decimals)) as amt1_float,
        em.tickLower,
        em.tickUpper
    from uniswap_v3_ethereum.Pair_evt_Mint em
    inner join uniswap_v3_ethereum.Factory_evt_PoolCreated pc
        on pc.pool = em.contract_address
    inner join tokens.erc20 tk0 
        on pc.token0 = tk0.contract_address
        and tk0.blockchain = 'ethereum'
    inner join tokens.erc20 tk1 
        on pc.token1 = tk1.contract_address
        and tk1.blockchain = 'ethereum'
    where 
        (em.amount0 =0 or em.amount1=0)
        and em.evt_block_time >= now() - interval '{{back_days}}' day
),
nft_add_liquidity as (
    -- aim of this CTE is to find tokenId
    select 
        evt_tx_hash
        , amount0
        , amount1
        , liquidity
        , tokenId
    from uniswap_v3_ethereum.NonfungibleTokenPositionManager_evt_IncreaseLiquidity
    where 
        (amount0=0 or amount1=0) -- add redundant constraints to reduce result size and speed up
        and evt_block_time >= now() - interval '{{back_days}}' day
),
nft_mint as (
    
)

select 
    pal.*,
    nal.liquidity,
    nal.tokenId
from pair_add_liquidity pal 
inner join nft_add_liquidity nal-- use inner join, mismatch is impossible
    -- TODO: assume one txn has only add liquidity once
    -- so when two event hash match, it means the same 'add liquidity' operation
    on pal.evt_tx_hash = nal.evt_tx_hash