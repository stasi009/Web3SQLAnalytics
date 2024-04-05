with airdrop as (
    select 
        date_trunc('day',min(block_time)) as start_claim_day
    from optimism.logs
    where block_date >= date '2024-02-16' -- day when airdrop contract is deployed
        and contract_address = 0xFb4D5A94b516DF77Fbdbcf3CfeB262baAF7D4dB7 -- airdrop contract
        and topic0 = 0x4ec90e965519d92681267467f775ada5bd214aa92c0dc93d90a5e880ce9ed026 -- claimed
)

, daily_last_stat as (
    select 
        day
        , total_voting_power
        , total_delegators
    from (
        select 
            date_trunc('day',block_time) as day
            , total_voting_power
            , total_delegators
            , row_number() over (partition by date_trunc('day',block_time) order by block_time desc) as rev_rank
        from op_governance_optimism.delegates
    )
    where rev_rank = 1
)

select 
    day
    , if(day >= ad.start_claim_day, 'After Airdrop', 'Before Airdrop') as period
    
    , total_voting_power
    , total_voting_power - lag(total_voting_power) ignore nulls over (order by day) as vote_power_diff

    , total_delegators
    , total_delegators - lag(total_delegators) ignore nulls over (order by day) as delegators_diff
    
from daily_last_stat
cross join airdrop ad 
where day >= ad.start_claim_day - interval '{{days_before_airdrop}}' day
    and day < current_date -- avoid incomplete date

