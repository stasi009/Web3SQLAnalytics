
with arbitrum_L1_fee as (
    select 
        date_trunc('hour',txns.block_timestamp) as hour 
        
        , count(txns.tx_hash) as num_txn

        , avg(gas_used) as avg_gas_used
        , avg(tx_fee) as avg_tx_fee

        , sum(gas_used) as sum_gas_used
        , sum(tx_fee) as sum_tx_fee

    from ethereum.core.fact_transactions txns
    where txns.to_address = lower('0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6') -- 'Arbitrum: Sequencer Inbox'
        and txns.block_timestamp::date >= '2024-03-12'
        and txns.block_timestamp < date_trunc('hour',current_timestamp) -- reduce impact of incomplete hour
    group by 1
)

, arbitrum_L2_fee as (
    select 
        date_trunc('hour',txns.block_timestamp) as hour 
        
        , count(txns.tx_hash) as num_txn

        , avg(gas_used) as avg_gas_used
        , avg(tx_fee) as avg_tx_fee

        , sum(gas_used) as sum_gas_used
        , sum(tx_fee) as sum_tx_fee

    from arbitrum.core.fact_transactions txns
    where txns.block_timestamp::date >= '2024-03-12'
        and txns.block_timestamp < date_trunc('hour',current_timestamp) -- reduce impact of incomplete hour
    group by 1
)


select 
    *
    , case 
        when hour <= timestamp '2024-03-13 13:55:59' then 'Before Dencun'
        else 'After Dencun'
    end as dencun_flag
from arbitrum_fee_on_L1
order by hour