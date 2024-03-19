-- https://dune.com/queries/3539411
with user_stake_dai as (
    select 
        user
        , sum(amount)/1e18 as amt_stake
    from query_3539341 --sq_stake_unstake_stablecoin.sql, https://dune.com/queries/3539341
    where action = 'stake stablecoin'
    group by 1
)

, user_unstake_dai as (
    select 
        user
        , sum(amount)/1e18 as amt_unstake
    from query_3539341 --sq_stake_unstake_stablecoin.sql, https://dune.com/queries/3539341
    where action = 'unstake dai request'
    group by 1
)

select 
    user
    , get_href(get_chain_explorer_address('ethereum', user),'etherscan') as link
    , amt_stake
    , amt_unstake  
    , amt_stake - amt_unstake as net_deposit
from (
    select 
        user
        , coalesce(amt_stake, 0) as amt_stake
        , coalesce(amt_unstake, 0) as amt_unstake   
    from user_stake_dai
    full outer join user_unstake_dai using (user)
)
order by net_deposit desc 
limit 30
