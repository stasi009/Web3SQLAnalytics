-- 某个delegate voting power的变化次数，与这个delegate投票次数的关系
with votes as (
    select 
        voter 
        , count(voter) as num_votes -- 不能用count(tx_hash)，来自snap的tx_hash都是null
    from governance_optimism.proposal_votes
    group by 1
)

, votes_range as (
    -- 这里将lower bound比actual minimum更小，upper bound比actual maximum更大
    -- 这样保证[lb, ub)能够囊括全部范围
    select 
        min(num_votes)-1 as min_votes
        -- upper bound is max(num_votes)+1
        -- lower bound is min(num_votes)-1
        , (max(num_votes) - min(num_votes)+2.0)/{{bins}} as bin_width
    from votes 
)

, votes_bins as (
    select 
        bin_idx
        , r.min_votes + (bin_idx-1)*r.bin_width as lb
        , r.min_votes + bin_idx * r.bin_width as ub
    from unnest(sequence(1,{{bins}})) as tbl(bin_idx)
    cross join votes_range r
)

, votes_binned as (
    select 
        voter
        , num_votes
        , b.bin_idx as votes_bin
    from votes v
    left join votes_bins b 
        on v.num_votes >= b.lb 
        and v.num_votes < b.ub
)

, vote_power_changes as (
    select 
        delegate 
        , count(tx_hash) as num_changes
    from op_governance_optimism.delegates del
    group by 1
)

select 
    v.votes_bin
    , count(v.voter) as num_voters
    , approx_percentile(num_changes, 0.5) as med_vote_power_chgs
from votes_binned v 
inner join vote_power_changes chg 
    on v.voter = chg.delegate
group by 1
order by 1