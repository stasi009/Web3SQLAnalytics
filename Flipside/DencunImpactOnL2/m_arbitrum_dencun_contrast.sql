with arbitrum_L1_fee as (
    select 
        date_trunc('hour',txns.block_timestamp) as hour 
        
        , count(txns.tx_hash) as num_txn
        , sum(gas_used) as sum_gas_used
        , sum(tx_fee) as sum_tx_fee

    from ethereum.core.fact_transactions txns
    where txns.to_address = lower('0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6') -- 'Arbitrum: Sequencer Inbox'
        and txns.block_timestamp >= '2024-03-10'
        and txns.block_timestamp < date_trunc('hour',current_timestamp) -- reduce impact of incomplete hour
    group by 1
)

, arbitrum_L2_fee as (
    select 
        date_trunc('hour',txns.block_timestamp) as hour 
        
        , count(txns.tx_hash) as num_txn
        , sum(gas_used) as sum_gas_used
        , sum(tx_fee) as sum_tx_fee

    from arbitrum.core.fact_transactions txns
    where txns.block_timestamp >= '2024-03-10'
        and txns.block_timestamp < date_trunc('hour',current_timestamp) -- reduce impact of incomplete hour
    group by 1
)

, dex_trades as (
    select 
        date_trunc('hour',block_timestamp) as hour 
        , count(tx_hash) as num_trades
        -- , sum(amount_in_usd + amount_out_usd)/2 as trade_volume
    from arbitrum.defi.ez_dex_swaps
    where txns.block_timestamp >= '2024-03-10'
        and txns.block_timestamp < date_trunc('hour',current_timestamp) -- reduce impact of incomplete hour
    group by 1
)

, lending_trades as (
    select 
        date_trunc('hour',block_timestamp) as hour 
        , count(tx_hash) as num_trades
        -- , sum(amount_usd) as trade_volume
    from arbitrum.defi.ez_lending_borrows
    where txns.block_timestamp >= '2024-03-10'
        and txns.block_timestamp < date_trunc('hour',current_timestamp) -- reduce impact of incomplete hour
    group by 1
)

, nft_trades as (
    select 
        date_trunc('hour',block_timestamp) as hour 
        , count(tx_hash) as num_trades -- 一次tx中可能有多笔交易，所以这里也不必去重
        -- , sum(price_usd) as trade_volume
    from arbitrum.nft.ez_nft_sales
    where txns.block_timestamp >= '2024-03-10'
        and txns.block_timestamp < date_trunc('hour',current_timestamp) -- reduce impact of incomplete hour
    group by 1
)