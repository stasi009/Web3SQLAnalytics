with airdrop as (
    select 
        date_trunc('day',min(block_time)) as start_claim_day
    from optimism.logs
    where block_date >= date '2024-02-16' -- day when airdrop contract is deployed
        and contract_address = 0xFb4D5A94b516DF77Fbdbcf3CfeB262baAF7D4dB7 -- airdrop contract
        and topic0 = 0x4ec90e965519d92681267467f775ada5bd214aa92c0dc93d90a5e880ce9ed026 -- claimed
)

, voting_power_daily_change as (
    select 
        day
        , daily_power_diff
        , sum(daily_power_diff) over (order by day) as total_voting_power
    from (
        select 
            date_trunc('day',block_time) as day 
            , sum(power_diff) as daily_power_diff
        from op_governance_optimism.voting_power
        group by 1
    )
)

select 
    day 
    , if(day >= ad.start_claim_day, 'After Airdrop', 'Before Airdrop') as period
    , daily_power_diff
    , total_voting_power
from voting_power_daily_change vpd
cross join airdrop ad 
where day >= ad.start_claim_day - interval '{{days_before_airdrop}}' day