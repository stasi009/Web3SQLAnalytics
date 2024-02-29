
with solana_user_first_day as (
    select 
        signer as user
        , min(block_date) as first_day
    from solana.transactions
    group by 1
)

, solana_txn_with_usertype as (
    select 
        txn.block_date
        , txn.id as txn_id
        , txn.signer as user

        , case  
            when txn.block_date = fd.first_day then true
            else false 
        end as is_new_user

    from solana.transactions txn 
    inner join solana_user_first_day fd -- I can inner join here because user_first_day derives from same dataset
        on txn.signer = fd.user
    where txn.block_date 
            between current_date - interval '{{back_days}}' day 
            and current_date - interval '1' day
)

, solana_txn_grpby_day_tmp as (
    select 
        block_date 

        , count(txn_id) filter (where is_new_user) as num_txn_new_users
        , count(txn_id) filter (where not is_new_user) as num_txn_old_users

        , count(distinct user) filter (where is_new_user) as num_new_users
        , count(distinct user) filter (where not is_new_user) as num_old_users
    from solana_txn_with_usertype
    group by 1
)
, solana_txn_grpby_day as (
    select
        'solana' as blockchain
        , block_date 

        , num_txn_new_users
        , num_txn_old_users
        , cast(num_txn_new_users as double) / (num_txn_new_users + num_txn_old_users) as new_user_txn_percent

        , num_new_users
        , num_old_users
        , cast(num_new_users as double) / (num_new_users + num_old_users) as new_user_percent
    from solana_txn_grpby_day_tmp
    order by block_date
)

select * from (
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

    select * from solana_txn_grpby_day
)
order by blockchain -- 固定顺序方便固定自动着色