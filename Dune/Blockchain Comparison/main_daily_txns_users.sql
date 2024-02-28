
with solana_daily_txns as (
    select 
        'solana' as blockchain
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
            , count(id) as num_txn -- id: txn id
            , count(id) filter (where success) as num_success_txn
            , count(distinct signer) as num_users
        from solana.transactions
        where block_date between current_date - interval '{{back_days}}' day and current_date - interval '1' day
        group by 1
    )   
)

select * from "query_3474945(evm_blockchain='ethereum',back_days='{{back_days}}')"

union all

select * from "query_3474945(evm_blockchain='arbitrum',back_days='{{back_days}}')"

union all

select * from "query_3474945(evm_blockchain='avalanche_c',back_days='{{back_days}}')"

union all

select * from "query_3474945(evm_blockchain='optimism',back_days='{{back_days}}')"

union all

select * from "query_3474945(evm_blockchain='polygon',back_days='{{back_days}}')"

union all

select * from solana_daily_txns