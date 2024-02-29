
with user_first_day as (
    select 
        "from" as user
        , min(block_date) as first_day
    -- 这里不能添加时间限制，我们需要全局的初次交易时间，而非近期的第1次交易时间
    from {{evm_blockchain}}.transactions txn
    group by 1
)

, txn_with_user_type as (
    select 
        txn.block_date
        , txn.hash as txn_hash
        , txn."from" as user

        , case  
            when txn.block_date = fd.first_day then true
            else false 
        end as is_new_user

    from {{evm_blockchain}}.transactions txn 
    inner join user_first_day fd -- I can inner join here because user_first_day derives from same dataset
        on txn."from" = fd.user
    where 
        txn.block_date between current_date - interval '{{back_days}}' day 
                        and current_date - interval '1' day
)

, txn_grpby_day as (
    select 
        block_date

        , count(txn_hash) filter (where is_new_user) as num_txn_new_users
        , count(txn_hash) filter (where not is_new_user) as num_txn_old_users

        , count(distinct user) filter (where is_new_user) as num_new_users
        , count(distinct user) filter (where not is_new_user) as num_old_users
    from txn_with_user_type
    group by 1
)

select
    '{{evm_blockchain}}' as blockchain
    , block_date 

    , num_txn_new_users
    , num_txn_old_users
    , cast(num_txn_new_users as double) / (num_txn_new_users + num_txn_old_users) as new_user_txn_percent

    , num_new_users
    , num_old_users
    , cast(num_new_users as double) / (num_new_users + num_old_users) as new_user_percent
from txn_grpby_day
order by block_date