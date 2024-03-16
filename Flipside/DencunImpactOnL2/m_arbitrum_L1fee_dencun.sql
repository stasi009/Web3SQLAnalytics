
with arbitrum_fee_on_L1 as (
    select 
        date_trunc('hour',txns.block_timestamp) as hour 
        
        , count(txns.tx_hash) as num_txn

        , avg(gas_used) as avg_gas_used
        , avg(tx_fee) as avg_tx_fee

    from ethereum.core.fact_transactions txns
    where txns.to_address = lower('0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6') -- 'Arbitrum: Sequencer Inbox'
    group by 1
)

, fee_before_dencun as (
    select 
        hour

        , num_txn as pre_dencun_num_txn
        , avg_gas_used as pre_dencun_avg_gas_used
        , avg_tx_fee as pre_dencun_avg_txfee

        , null as post_dencun_num_txn
        , null as post_dencun_avg_gas_used
        , null as post_dencun_avg_txfee

    from arbitrum_fee_on_L1
    where hour::date >= '2024-03-12'
        and hour <= timestamp '2024-03-13 13:55:59'
    order by 1
)

, fee_after_dencun as (
    select 
        hour

        , null as pre_dencun_num_txn
        , null as pre_dencun_avg_gas_used
        , null as pre_dencun_avg_txfee

        , num_txn as post_dencun_num_txn
        , avg_gas_used as post_dencun_avg_gas_used
        , avg_tx_fee as post_dencun_avg_txfee

    from arbitrum_fee_on_L1
    where hour > timestamp '2024-03-13 13:55:59'
        and hour < date_trunc('hour',current_timestamp) -- reduce impact of incomplete hour
    order by 1
)

select * from fee_before_dencun
union all
select * from fee_after_dencun


