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
            , rank() over (partition by delegate order by block_time desc) as latest_rank
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
-- , total_vote_power as (
--     select 
--         total_voting_power
--         , total_delegators
--     from (
--         select 
--             total_voting_power
--             , total_delegators
--             , rank() over (order by block_time desc) as latest_rank
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

select * from total_vote_power