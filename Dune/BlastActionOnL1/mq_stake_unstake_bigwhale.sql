
with user_stake_eth as (
    select 
        user
        , sum(amount)/1e18 as amt_stake_eth
    from query_3537077 -- sq_stake_unstake_eth.sql, https://dune.com/queries/3537077
    where action = 'stake ETH'
    group by 1
)

, user_stake_steth as (
    select 
        user
        , sum(amount)/1e18 as amt_stake_steth
    from query_3537077 -- sq_stake_unstake_eth.sql, https://dune.com/queries/3537077
    where action = 'stake stETH'
    group by 1
)

, user_unstake as (
    select 
        user
        , sum(amount)/1e18 as amt_unstake
    from query_3537077 -- sq_stake_unstake_eth.sql, https://dune.com/queries/3537077
    where action = 'unstake request'
    group by 1
)

select 
    user
    , get_href(get_chain_explorer_address('ethereum', user),'etherscan') as link
    , amt_stake_eth
    , amt_stake_steth
    , amt_unstake  
    , amt_stake_eth + amt_stake_steth - amt_unstake as net_deposit
from (
    select 
        user
        , coalesce(amt_stake_eth, 0) as amt_stake_eth
        , coalesce(amt_stake_steth, 0) as amt_stake_steth
        , coalesce(amt_unstake, 0) as amt_unstake   
    from user_stake_eth
    full outer join user_stake_steth using (user)
    full outer join user_unstake using (user)
)
order by net_deposit desc 
limit 30
