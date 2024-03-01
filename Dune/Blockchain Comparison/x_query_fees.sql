with ethereum_fees as (
    select 
        get_href(get_chain_explorer_tx_hash('ethereum', t.hash), 'link') as link
        , t.type
        , t.gas_used
        , t.gas_price -- orginal value in wei
        , t.gas_price / 1e9 as gas_price_in_gwei
        , b.base_fee_per_gas
        , t.priority_fee_per_gas
        , (t.gas_price - b.base_fee_per_gas - t.priority_fee_per_gas = 0) as gas_price_matched
        , t.gas_price / 1e18 * t.gas_used as txn_fee_eth
    from ethereum.transactions t
    inner join ethereum.blocks b
        on t.block_number = b.number
    where block_date = current_date 
        and success
)

, arbitrum_fees as (
    select 
        -- t.hash
        get_href(get_chain_explorer_tx_hash('arbitrum', t.hash), 'link') as link
        , t.type

        , t.gas_used
        , t.gas_used_for_l1
        
        , t.gas_price / 1e9 as gas_price_gwei
        , t.effective_gas_price / 1e9 as effective_gas_price_gwei

        -- * NOTE: t.gas_price / 1e18 * t.gas_used as txn_fee_eth
        , t.effective_gas_price / 1e18 * t.gas_used as txn_fee_eth

        , b.base_fee_per_gas / 1e9 as base_fee_per_gas_gwei
        , t.priority_fee_per_gas / 1e9 as priority_fee_per_gas_gwei

        -- * NOTE: both following statements NOT always true
        , (t.gas_price = b.base_fee_per_gas + t.priority_fee_per_gas) as gas_price_matched
        , (t.effective_gas_price = b.base_fee_per_gas + t.priority_fee_per_gas) as effective_gas_price_matched

    from arbitrum.transactions t
    inner join arbitrum.blocks b
        on t.block_number = b.number
    where block_date = current_date 
        and success
        and t.gas_price <> t.effective_gas_price -- I want to focus on these special cases
        and t.gas_used > 0
)

, optimism_fees as (
    select 
        -- t.hash
        get_href(get_chain_explorer_tx_hash('optimism', t.hash), 'link') as link
        , (t.l1_gas_price * t.l1_gas_used * t.l1_fee_scalar + t.gas_used*t.gas_price)/1e18 as txn_fee_eth

        , t.gas_used        
        , t.gas_price / 1e9 as gas_price_gwei

        , t.l1_gas_used
        , t.l1_gas_price / 1e9 as l1_gas_price_gwei

        , b.base_fee_per_gas / 1e9 as base_fee_per_gas_gwei
        , t.priority_fee_per_gas / 1e9 as priority_fee_per_gas_gwei
        , (t.gas_price = b.base_fee_per_gas + t.priority_fee_per_gas) as gas_price_matched

        -- * NOTE: almost zero, so l1_fee=l1_gas_price * l1_gas_used * l1_fee_scalar
        -- difference < 1wei
        , (t.l1_gas_price * t.l1_gas_used * t.l1_fee_scalar - t.l1_fee) as calc_l1_fee_diff

    from optimism.transactions t
    inner join optimism.blocks b
        on t.block_number = b.number
    where block_date = current_date 
        and success
        and t.gas_used > 0
)

, avalanche_c_fee as (
    select 
        -- t.hash
        get_href(get_chain_explorer_tx_hash('avalanche_c', t.hash), 'link') as link
        , t.gas_used*t.gas_price /1e18 as txn_fee_avax

        , t.gas_used        
        , t.gas_price / 1e9 as gas_price_gwei

        , b.base_fee_per_gas / 1e9 as base_fee_per_gas_gwei
        , t.priority_fee_per_gas / 1e9 as priority_fee_per_gas_gwei
        -- ? priority_fee_per_gas is null, but gas_price > b.base_fee_per_gas
        , (t.gas_price = b.base_fee_per_gas + t.priority_fee_per_gas) as gas_price_matched

    from avalanche_c.transactions t
    inner join avalanche_c.blocks b
        on t.block_number = b.number
    where block_date = current_date 
        and success
        and t.gas_used > 0
)

, polygon_fee as (
    select 
        -- t.hash
        get_href(get_chain_explorer_tx_hash('polygon', t.hash), 'link') as link
        , t.gas_used*t.gas_price /1e18 as txn_fee_matic

        , t.gas_used        
        , t.gas_price / 1e9 as gas_price_gwei


        , b.base_fee_per_gas / 1e9 as base_fee_per_gas_gwei
        , t.priority_fee_per_gas / 1e9 as priority_fee_per_gas_gwei
        -- ? priority_fee_per_gas is null, but gas_price > b.base_fee_per_gas
        , (t.gas_price = b.base_fee_per_gas + t.priority_fee_per_gas) as gas_price_matched

    from polygon.transactions t
    inner join polygon.blocks b
        on t.block_number = b.number
    where block_date = current_date 
        and success
        and t.gas_used > 0
)

, solana_fee as (
    select 
        -- t.hash
        get_href(get_chain_explorer_tx_hash('solana', t.id), 'link') as link
        , t.fee / 1e9 as txn_fee_sol

    from solana.transactions t
    where block_date = current_date 
        and success
)

select * from solana_fee
limit 10
