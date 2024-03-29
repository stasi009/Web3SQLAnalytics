select
    '{{evm_blockchain}}' as blockchain
    , block_date 
    , num_txn
    , num_success_txn
    , cast(num_success_txn as double) / num_txn as txn_success_rate
    , num_txn / 86400.0 as txn_per_sec
    , num_success_txn / 86400.0 as succ_txn_per_sec
    , num_users
from (
    select 
        block_date
        , count(hash) as num_txn
        , count(hash) filter (where success) as num_success_txn
        , count(distinct "from") as num_users
    from {{evm_blockchain}}.transactions txn
    -- between include both ends
    where block_date between current_date - interval '{{back_days}}' day and current_date - interval '1' day
    group by 1
)