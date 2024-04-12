
with vote_power_changes as (
    select 
        delegate 
        , count(tx_hash) as num_changes
    from op_governance_optimism.delegates del
    inner join op_governance_optimism.delegates_addresses as adr 
        on del.delegate = adr.address -- 只考虑那些大delegate，不考虑小的delegate，排除噪声
    group by 1
    order by num_changes desc
)
, votes as (
    select 
        voter 
        , count(voter) as num_votes -- 不能用count(tx_hash)，来自snap的tx_hash都是null
    from governance_optimism.proposal_votes
    group by 1
)

select 
    pchg.delegate 
    , pchg.num_changes as vote_power_chgs 
    , v.num_votes
from vote_power_changes pchg
inner join votes v 
    on pchg.delegate = v.voter
order by vote_power_chgs desc