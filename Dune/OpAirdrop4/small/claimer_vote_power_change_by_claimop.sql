-- 在claimed op的不同区间，change summary的分布
with opt_amt_range as (
    -- 这里将lower bound比actual minimum更小，upper bound比actual maximum更大
    -- 这样保证[lb, ub)能够囊括全部范围
    select 
        min(total_op)*0.99 as min_amt
        , (max(total_op)*1.01 - min(total_op)*0.99)/{{bins}} as bin_width
    from dune.oplabspbc.dataset_op_airdrop_4_simple_list -- all addresses qualified for airdrop4
)

, op_amt_bins as (
    select 
        bin_idx
        , r.min_amt + (bin_idx-1)*r.bin_width as lb
        , r.min_amt + bin_idx * r.bin_width as ub
    from unnest(sequence(1,{{bins}})) as tbl(bin_idx)
    cross join opt_amt_range r
)

select 
    change_summary
    , count(claimer) as num_claimers
    , sum(claim_op) as claim_op

    , sum(vote_power_pre_ad) as vote_power_pre_ad
    , sum(vote_power_post_ad) as vote_power_post_ad
    , sum(vote_power_post_ad) - sum(vote_power_pre_ad) as vote_power_change
from query_3598102 -- claimer_vote_power_change.sql
group by change_summary

