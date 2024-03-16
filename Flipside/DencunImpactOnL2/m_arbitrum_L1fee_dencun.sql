
with arbitrum_fee_on_L1 as (
    select 
        date_trunc('hour',txns.block_timestamp) as hour 
        
        , count(txns.tx_hash) as num_txn

        , avg(gas_used) as avg_gas_used
        , avg(tx_fee) as avg_tx_fee_eth

        , APPROX_PERCENTILE(gas_used,0.5) as median_gas_used
        , APPROX_PERCENTILE(tx_fee,0.5) as median_tx_fee_eth

        , avg(gas_price) as avg_gas_price_gwei
    from ethereum.core.fact_transactions txns
    where txns.to_address = lower('0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6') -- 'Arbitrum: Sequencer Inbox'
        and txns.block_timestamp::date >= '2024-03-12'
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