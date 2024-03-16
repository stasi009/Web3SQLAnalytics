with ArbitrumProxyOnL1 as (
    select * from 
    values

        ('0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6', 'Arbitrum: Sequencer Inbox')
        -- , ('0x51de512aa5dfb02143a91c6f772261623ae64564', 'Arbitrum: Validator1')
        -- , ('0x72Ce9c846789fdB6fC1f34aC4AD25Dd9ef7031ef', 'Arbitrum: Gateway Router')

    as my_table(address, name)
)

, fee_paid_on_L1 as (
    select 
        date_trunc('hour',txns.block_timestamp) as hour 
        
        , count(txns.tx_hash) as num_txn

        , avg(gas_used) as avg_gas_used
        , avg(tx_fee) as avg_tx_fee_eth

        , APPROX_PERCENTILE(gas_used,0.5) as median_gas_used
        , APPROX_PERCENTILE(tx_fee,0.5) as median_tx_fee_eth

        , avg(gas_price) as avg_gas_price_gwei
    from ethereum.core.fact_transactions txns
    inner join ArbitrumProxyOnL1 l2
        on txns.to_address = lower(l2.address) 
    where txns.block_timestamp::date >= '2024-03-12'
        and txns.block_timestamp < date_trunc('hour',current_timestamp) -- reduce impact of incomplete hour
    group by 1
)


