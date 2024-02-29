
with user_first_day as (
    select 
        "from" as user
        , min(block_date) as first_day
    from {{evm_blockchain}}.transactions txn
    -- between include both ends
    where block_date between current_date - interval '{{back_days}}' day 
                    and current_date - interval '1' day
    group by 1
)

, txn_grpby_day_usertype as (
    select 
        txn.block_date

        , case  
            when txn.block_date = fd.first_day then 'new'
            else 'old' end 
        as user_new_old

        , count(txn.hash) as num_txn
        , count(txn.hash) filter (where success) as num_success_txn
        , count(distinct txn."from") as num_users
    from {{evm_blockchain}}.transactions txn 
    inner join user_first_day fd -- I can use inner join here because user_first_day come from the same query
        on txn."from" = fd.user
        -- apply fiter condition during join, this can reduce join size
        -- this can work, because I am using a inner join
        and txn.block_date between current_date - interval '{{back_days}}' day and current_date - interval '1' day
    group by 1,2
)

select
    '{{evm_blockchain}}' as blockchain
    , block_date 
    , user_new_old

    , num_txn
    , num_success_txn
    , cast(num_success_txn as double) / num_txn as txn_success_rate

    , num_txn / 86400.0 as txn_per_sec
    , num_success_txn / 86400.0 as succ_txn_per_sec
    
    , num_users
from txn_grpby_day_usertype