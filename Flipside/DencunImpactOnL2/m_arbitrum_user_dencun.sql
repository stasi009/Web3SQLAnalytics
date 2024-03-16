
with first_txn_day as (
    select 
        from_address as user
        , min(date_trunc('day',block_timestamp)) as first_day
    from arbitrum.core.fact_transactions txns
    -- 如果用户两年前发生第一笔交易，然后就消失了，最近某天突然再使用，我还认为他是新用户
    where txns.block_timestamp >= current_timestamp - interval '1 year'
    group by 1
)

, recent_daily_unique_users as (
    select 
        date_trunc('day',txns.block_timestamp) as day 
        , from_address as user
    from arbitrum.core.fact_transactions txns
    where txns.block_timestamp::date >= date_trunc('day',current_timestamp) - interval '14 day'
        and txns.block_timestamp < date_trunc('day',current_timestamp) -- reduce impact of incomplete day
    group by 1,2
)

, daily_user_nums as (
    select 
        u.day
        , count(u.user) as active_users -- 已经在生成daily_unique_users时去过重了
        , count(case when u.day = d0.first_day then u.user else null end) as new_users 
    from recent_daily_unique_users u
    inner join first_txn_day d0
        on u.user = d0.user
    group by 1
)

select 
    *
    , case 
        when day <= date '2024-03-12' then 'Before Dencun' -- upgrade happen at '2024-03-13 13:55:59'
        else 'After Dencun'
    end as dencun_flag
from daily_user_nums
order by day
