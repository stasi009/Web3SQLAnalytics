
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
        and txns.block_timestamp::date >= '2024-03-10'
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
    where txns.block_timestamp::date >= '2024-03-10'
        and txns.block_timestamp < date_trunc('hour',current_timestamp) -- reduce impact of incomplete hour
    group by 1
)

select 
    hour  

    , case 
        when hour <= timestamp '2024-03-13 13:55:59' then 'Before Dencun'
        when hour <= timestamp '2024-03-14 14:50' then 'Not Active On Arbitrum'
        else 'Dencun Active On Arbitrum'
    end as dencun_flag
    
    , l1.num_txn as l1_num_txn
    , l1.avg_gas_used as l1_avg_gas_used
    , l1.avg_tx_fee as l1_avg_tx_fee
    , l1.sum_gas_used as l1_sum_gas_used
    , l1.sum_tx_fee as l1_sum_tx_fee

    , l2.num_txn as l2_num_txn
    , l2.avg_gas_used as l2_avg_gas_used
    , l2.avg_tx_fee as l2_avg_tx_fee
    , l2.sum_gas_used as l2_sum_gas_used
    , l2.sum_tx_fee as l2_sum_tx_fee

    , (l2_sum_tx_fee - l1_sum_tx_fee) as profit

from arbitrum_L1_fee l1 
inner join arbitrum_L2_fee l2 
    using (hour)
order by 1


