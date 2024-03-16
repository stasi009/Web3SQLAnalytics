
with arbitrum_txns_on_L1 as (
    select 
        date_trunc('hour',txns.block_timestamp) as hour 
        , tx_hash
        , case 
            when array_size(blob_versioned_hashes) > 0 and tx_type=3 then 1 
            else 0
        end as is_blob_txn

    from ethereum.core.fact_transactions txns
    where txns.from_address = lower('0xC1b634853Cb333D3aD8663715b08f41A3Aec47cc') -- from Arbitrum: Batch Submitter
        and block_timestamp::date >= '2024-03-12' 
        and block_timestamp < date_trunc('hour',current_timestamp) -- reduce impact of incomplete hour
)

select 
    hour
    , count(tx_hash) as total_txn
    , sum(is_blob_txn) as total_blob_txn
    , count(tx_hash) - sum(is_blob_txn) as total_non_blob_txn
from arbitrum_txns_on_L1
group by 1
order by 1
