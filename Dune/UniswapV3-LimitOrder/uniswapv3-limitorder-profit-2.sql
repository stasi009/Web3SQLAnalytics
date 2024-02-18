
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
        --------------------------------
        , nft.symbol0
        , (il.amount0 / power(10, nft.tk0decimal)) as amt0_float
        , (il.amount0 / power(10, nft.tk0decimal) * p0.price) as amt0_usd
        , nft.symbol1
        , (il.amount1 / power(10, nft.tk1decimal)) as amt1_float
        , (il.amount1 / power(10, nft.tk1decimal) * p1.price) as amt1_usd
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
        and 
        -- there is some dirty data that both amounts are zero
        ((il.amount0=0 and il.amount1>0) or (il.amount0>0 and il.amount1=0))
),
remove_liquidity as (
    select 
        dl.evt_tx_hash as tx_hash,
        dl.evt_block_time as block_time,
        nft.pair_symbol,
        --------------------------------
        nft.symbol0,
        (dl.amount0 / power(10, nft.tk0decimal)) as amt0_float,
        (dl.amount0 / power(10, nft.tk0decimal) * p0.price) as amt0_usd,
        nft.symbol1,
        (dl.amount1 / power(10, nft.tk1decimal)) as amt1_float,
        (dl.amount1 / power(10, nft.tk1decimal) * p1.price) as amt1_usd,
        --------------------------------
        dl.liquidity,
        dl.tokenId
    from uniswap_v3_ethereum.NonfungibleTokenPositionManager_evt_DecreaseLiquidity dl 
    inner join nft_tokens nft 
        on dl.tokenId = nft.tokenId
    inner join prices p0
        on p0.contract_address = nft.token0
        and p0.minute = date_trunc('minute',dl.evt_block_time)
    inner join prices p1
        on p1.contract_address = nft.token1
        and p1.minute = date_trunc('minute',dl.evt_block_time)
    where 
        dl.evt_block_time >= now() - interval '{{back_days}}' day
),
limitorder_profit_status as (
    select
        al.tokenId,
        al.pair_symbol,
        al.block_time as addliq_time,
        rl.block_time as rmvliq_time,

        al.amt0_float as add_float0,
        al.amt0_usd as add_usd0,
        al.amt1_float as add_float1,
        al.amt1_usd as add_usd1,
        al.liquidity as add_liquidity,

        rl.amt0_float as rmv_float0,
        rl.amt0_usd as rmv_usd0,
        rl.amt1_float as rmv_float1,
        rl.amt1_usd as rmv_usd1,
        rl.liquidity as rmv_liquidity,

        (rl.amt0_usd + rl.amt1_usd - al.amt0_usd - al.amt1_usd) as pnl,
        (rl.amt0_usd + rl.amt1_usd)/ (al.amt0_usd + al.amt1_usd)-1 as pnl_percent,

        case 
            -- add liquidity (x=0,y>0), remove liquidity (x>0, y=0), or
            -- add liquidity (x>0,y=0), remove liquidity (x=0, y>0)
            when al.amt0_float * rl.amt0_float =0 and al.amt1_float * rl.amt1_float =0 then 'complete_order'
            -- add liquidity (x=0,y>0), remove liquidity (x=0, y>0)
            when al.amt0_float + rl.amt0_float =0 and al.amt1_float * rl.amt1_float >0 then 'revoke_order'
            -- add liquidity (x>0,y=0), remove liquidity (x>0, y=0)
            when al.amt0_float * rl.amt0_float >0 and al.amt1_float + rl.amt1_float =0 then 'revoke_order'
            -- add liquidity (x>0,y=0), remove liquidity (x>0, y>0)
            when al.amt0_float * rl.amt0_float >0 and al.amt1_float * rl.amt1_float =0 then 'partial_order'
            -- add liquidity (x=0,y>0), remove liquidity (x>0, y>0)
            when al.amt0_float * rl.amt0_float =0 and al.amt1_float * rl.amt1_float >0 then 'partial_order'
            when rl.amt0_float is null and rl.amt1_float is null then 'order_still_open'
        end as order_status,

        date_diff('hour',al.block_time, rl.block_time) as elapsed_hours
        -- case 
        --     when rl.liquidity = al.liquidity then 'rmv_all_liq'
        --     when rl.liquidity < al.liquidity then 'rmv_partial_liq'
        --     -- there must be a 'increase liquidity' txn out of the query time range
        --     -- and it remove all combined liquidity this time
        --     when rl.liquidity > al.liquidity then 'rmv_outofrange_liq' 
        -- end as rmv_liq_type
    from add_single_liquidity al 
    left join remove_liquidity rl 
        on al.tokenId = rl.tokenId 
        and al.pair_symbol = rl.pair_symbol 
        -- only care about removing all liquidity placed by limit order previously
        -- this can limit the 'remove liquidity' order to only one previous 'add liquidity' order (most of the time)
        -- not accurate, but simple
        and al.liquidity = rl.liquidity 
        and rl.block_time > al.block_time
)

-- select 
--     liquid_provider,
--     sum(amt0_usd + amt1_usd) as total_limitorder_vol,
--     count(tx_hash) as total_limitorder_txn
-- from add_single_liquidity
-- group by 1
-- order by total_limitorder_vol desc


select 
    order_status,
    count(1) as num_orders,
    avg(case when pnl>0 then 1 else 0 end) as win_rate,
    approx_percentile(pnl_percent,0.5) as median_profit_percent,
    approx_percentile(elapsed_hours,0.5) as median_elapsed_hours
from limitorder_profit_status
group by 1
order by num_orders desc

-- select 
--     pair_symbol,
--     count(1) as num_addliq,
--     avg(pnl) as avg_pnl,
--     approx_percentile(pnl,0.5) as median_pnl,
--     avg(pnl_percent) as avg_pnl_percent,
--     approx_percentile(pnl_percent,0.5) as median_pnl_percent
-- from limitorder_profit_status
-- group by 1
-- order by num_addliq desc
