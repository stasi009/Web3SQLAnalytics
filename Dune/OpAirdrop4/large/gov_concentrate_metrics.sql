-- https://dune.com/queries/3615639
-- 计算delegate voting power的集中度
with first_airdrop_claim as (
    select 
        min(evt_block_time) as first_claim_tm
    from optimism_airdrop_4_optimism.MerkleDistributor_evt_Claimed
)

, latest_voting_power as (
    select 
        delegate
        , current_voting_power
        , number_of_delegators
    from (
        select 
            delegate
            , current_voting_power
            , number_of_delegators
            , row_number() over (partition by delegate order by block_time desc) as latest_rank
        from op_governance_optimism.delegates del
        cross join first_airdrop_claim fc
        where 
            case
                when '{{mode}}' = 'current' then true 
                when '{{mode}}' = 'before_airdrop' then del.block_time < fc.first_claim_tm
            end
    )
    where latest_rank = 1
)

, total_vote_power as (
    select 
        sum(current_voting_power) as total_voting_power
        , sum(number_of_delegators) as total_delegators
    from latest_voting_power
)

-- 换一种方式计算total_voting_power和total_delegators,结果应该一致
-- !但是实际计算结果有误差（一个可能原因是，如果block_time相同时有若干voting power change，那么row_number=1返回的是哪一个？？）
-- , total_vote_power as (
--     select 
--         total_voting_power
--         , total_delegators
--     from (
--         select 
--             total_voting_power
--             , total_delegators
--             , row_number() over (order by block_time desc) as latest_rank
--         from op_governance_optimism.delegates del
--         cross join first_airdrop_claim fc
--         where 
--             case
--                 when '{{mode}}' = 'current' then true 
--                 when '{{mode}}' = 'before_airdrop' then del.block_time < fc.first_claim_tm
--             end
--     )
--     where latest_rank = 1
-- )

, latest_voting_power_with_percentage as (
    select 
        delegate 

        , current_voting_power
        , current_voting_power * 100.0 / total_voting_power as voting_power_percentage
        , rank() over (order by current_voting_power desc) vote_power_rank 

        , number_of_delegators
        , number_of_delegators * 100.0 / total_delegators as delegator_percentage
        , rank() over (order by number_of_delegators desc) delegator_rank 

    from latest_voting_power
    cross join total_vote_power
)

, hhi as (
    select 
        sum(power(voting_power_percentage,2)) as voting_power_hhi
        , sum(power(delegator_percentage, 2)) as delegators_hhi
    from latest_voting_power_with_percentage
)

, top_vote_power_coverage as (
    select 
        sum(voting_power_percentage) as topn_vote_power_percentage
    from latest_voting_power_with_percentage
    where vote_power_rank <= {{topn_delegates}}
)

, top_delegator_coverage as (
    select 
        sum(delegator_percentage) as topn_delegator_percentage
    from latest_voting_power_with_percentage
    where delegator_rank <= {{topn_delegates}}
)

select 
    tt.*
    , h.* 
    , tvp.*  
    , td.*
from total_vote_power tt
cross join hhi h 
cross join top_vote_power_coverage tvp 
cross join top_delegator_coverage td