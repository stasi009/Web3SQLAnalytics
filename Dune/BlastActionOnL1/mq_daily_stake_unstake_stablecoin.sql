with daily_stats as (
    select 
        block_date 
        , action 
        , count(tx_hash) as tx_num
        , count(distinct user) as num_users
        , sum(amount)/1e18 as dai_amount
    from query_3539341 --sq_stake_unstake_stablecoin.sql, https://dune.com/queries/3539341
    group by 1,2
)

select 
    block_date 
    , action 
    , case when action = 'unstake dai request' then -1 else 1 end * tx_num as tx_num 
    , case when action = 'unstake dai request' then -1 else 1 end * num_users as num_users
    , case when action = 'unstake dai request' then -1 else 1 end * dai_amount as dai_amount
from daily_stats
order by 1
