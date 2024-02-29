
with solana_user_first_day as (
    select 
        signer as user
        , min(block_date) as first_day
    from solana.transactions
    group by 1
)

, solana_txn_grpby_day_usertype_tmp as (
    select 
        txn.block_date

        , case  
            when txn.block_date = fd.first_day then 'new'
            else 'old' end 
        as user_new_old

        , count(id) as num_txn -- id: txn id
        , count(id) filter (where success) as num_success_txn
        , count(distinct signer) as num_users
    from solana.transactions txn 
    inner join solana_user_first_day fd -- I can inner join here because user_first_day derives from same dataset
        on txn.signer = fd.user
        -- apply fiter condition during join, this can reduce join size
        -- this can work, because I am using a inner join
        and txn.block_date between current_date - interval '{{back_days}}' day and current_date - interval '1' day
    group by 1,2
)

, solana_txn_grpby_day_usertype as (
    select 
        'solana' as blockchain
        , block_date 
        , user_new_old

        , num_txn
        , num_success_txn
        , cast(num_success_txn as double) / num_txn as txn_success_rate

        , num_txn / 86400.0 as txn_per_sec
        , num_success_txn / 86400.0 as succ_txn_per_sec

        , num_users
    from solana_txn_grpby_day_usertype_tmp
)

select * from "query_3477050(evm_blockchain='ethereum',back_days='{{back_days}}')"

union all

select * from "query_3477050(evm_blockchain='arbitrum',back_days='{{back_days}}')"

union all

select * from "query_3477050(evm_blockchain='avalanche_c',back_days='{{back_days}}')"

union all

select * from "query_3477050(evm_blockchain='optimism',back_days='{{back_days}}')"

union all

select * from "query_3477050(evm_blockchain='polygon',back_days='{{back_days}}')"

union all

select * from solana_txn_grpby_day_usertype