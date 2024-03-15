with ArbitrumProxyOnL1 as (
    select * from 
    values

        ('0x4c6f947Ae67F572afa4ae0730947DE7C874F95Ef', 'Arbitrum: Sequencer Inbox')
        , ('0x51de512aa5dfb02143a91c6f772261623ae64564', 'Arbitrum: Validator1')
        , ('0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6', 'Arbitrum: Sequencer Inbox NITRO')
        , ('0x72Ce9c846789fdB6fC1f34aC4AD25Dd9ef7031ef', 'Arbitrum: Gateway Router')

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

        , sum(gas_used) as sum_gas_used
        , sum(tx_fee) as sum_tx_fee_eth

        , avg(gas_price) as avg_gas_price_gwei
    from ethereum.core.fact_transactions txns
    left join ArbitrumProxyOnL1 l2
        on txns.to_address = l2.address 
    where txns.block_timestamp::date >= '2024-03-12'
        -- reduce impact of incomplete hour
        and txns.block_timestamp < date_trunc('hour',current_timestamp) - interval '1 hour'
    group by 1
)

, fee_recv_on_L2 as (
    select 
        date_trunc('hour',txns.block_timestamp) as hour 
        
        , count(txns.tx_hash) as num_txn

        , avg(gas_used) as avg_gas_used
        , avg(tx_fee) as avg_tx_fee_eth

        , APPROX_PERCENTILE(gas_used,0.5) as median_gas_used
        , APPROX_PERCENTILE(tx_fee,0.5) as median_tx_fee_eth

        , sum(gas_used) as sum_gas_used
        , sum(tx_fee) as sum_tx_fee_eth

        , avg(gas_price_paid) as avg_gas_price_gwei
    from arbitrum.core.fact_transactions txns
    where txns.block_timestamp::date >= '2024-03-12'
        -- reduce impact of incomplete hour
        and txns.block_timestamp < date_trunc('hour',current_timestamp) - interval '1 hour'
    group by 1
)

select 
    l1.hour  
    
    , l1.num_txn as l1_num_txn
    , l1.avg_gas_used as l1_avg_gas_used
    , l1.avg_tx_fee_eth as l1_avg_tx_fee_eth
    , l1.sum_gas_used as l1_sum_gas_used
    , l1.sum_tx_fee_eth as l1_sum_tx_fee_eth
    , l1.avg_gas_price_gwei as l1_avg_gas_price_gwei

    , l2.num_txn as l2_num_txn
    , l2.avg_gas_used as l2_avg_gas_used
    , l2.avg_tx_fee_eth as l2_avg_tx_fee_eth
    , l2.sum_gas_used as l2_sum_gas_used
    , l2.sum_tx_fee_eth as l2_sum_tx_fee_eth
    , l2.avg_gas_price_gwei as l2_avg_gas_price_gwei
from fee_paid_on_L1 l1 
inner join fee_recv_on_L2 l2 
    on l1.hour = l2.hour
order by 1