with arbitrum_L1_fee as (
    select 
        date_trunc('hour',txns.block_timestamp) as hour 
        
        , count(txns.tx_hash) as num_txn

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

    from arbitrum.core.fact_transactions txns
    where txns.block_timestamp >= '2024-03-10'
        and txns.block_timestamp < date_trunc('hour',current_timestamp) -- reduce impact of incomplete hour
    group by 1
)

, before_after_metrics as (
    select 
        case 
            when hour <= timestamp '2024-03-14 14:50' then false
            else true
        end as after_dencun

        , avg(l1.num_txn) as l1_hourly_txns
        , avg(l2.num_txn) as l2_hourly_txns

    from arbitrum_L1_fee l1 
    inner join arbitrum_L2_fee l2 using (hour)
    group by 1
)

, rotate_before_metrics as (
    select 1 as rowidx, 'Hourly Txns on L1' as metric, l1_hourly_txns as "Before" from before_after_metrics where not after_dencun
    
    union all

    select 4 as rowidx, 'Hourly Txns on L2' as metric, l2_hourly_txns as "Before" from before_after_metrics where not after_dencun
)

, rotate_after_metrics as (
    select 1 as rowidx, 'Hourly Txns on L1' as metric, l1_hourly_txns as "After" from before_after_metrics where after_dencun
   
    union all

    select 4 as rowidx, 'Hourly Txns on L2' as metric, l2_hourly_txns as "After" from before_after_metrics where after_dencun
)

select 
    metric
    , round("Before",2) as "Before"
    , round("After",2) as "After"
    , round(("After" / "Before" - 1)*100,2)::string || '%' as "Change Percentage %"
from rotate_before_metrics
inner join rotate_after_metrics
    using (rowidx, metric)
order by rowidx
