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

        -- !!! NOT t.gas_price / 1e18 * t.gas_used as txn_fee_eth
        , t.effective_gas_price / 1e18 * t.gas_used as txn_fee_eth

        , b.base_fee_per_gas / 1e9 as base_fee_per_gas_gwei
        , t.priority_fee_per_gas / 1e9 as priority_fee_per_gas_gwei
        -- !!! NOTE: NOT gas_price, some gas_price are 0
        -- , (t.gas_price - b.base_fee_per_gas - t.priority_fee_per_gas = 0) as gas_price_matched
        , (t.effective_gas_price - b.base_fee_per_gas - t.priority_fee_per_gas = 0) as effective_gas_price_matched

    from arbitrum.transactions t
    inner join arbitrum.blocks b
        on t.block_number = b.number
    where block_date = current_date 
        and success
        and t.gas_price <> t.effective_gas_price -- I want to focus on these special cases
        and t.gas_used > 0
)

, 

select * from arbitrum_fees
limit 10
