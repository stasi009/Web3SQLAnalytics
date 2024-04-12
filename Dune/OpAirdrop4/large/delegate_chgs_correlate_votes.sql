with vote_power_changes as (
    select 
        delegate 
        , ln(count(tx_hash)) as num_changes
    from op_governance_optimism.delegates del
    group by 1
)

, changes_range as (
    -- 这里将lower bound比actual minimum更小，upper bound比actual maximum更大
    -- 这样保证[lb, ub)能够囊括全部范围
    select 
        min(num_votes)*0.99 as min_votes
        -- upper bound is max(num_votes)+1
        -- lower bound is min(num_votes)-1
        , (max(num_votes)*1.01 - min(num_votes)*0.99)/{{bins}} as bin_width
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