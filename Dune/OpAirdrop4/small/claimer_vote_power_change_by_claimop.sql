-- https://dune.com/queries/3598567
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

, claimer_vote_power_changes_with_claimop_bin as (
    select 
        claimer
        , change_summary

        , claim_op
        , bins.bin_idx
        , bins.lb as bin_lb
        , bins.ub as bin_ub

        , delegate_before_airdrop
        , delegate_after_airdrop

        , op_before_airdrop
        , op_after_airdrop

        , vote_power_pre_ad
        , vote_power_post_ad
    from query_3598102 -- claimer_vote_power_change.sql
    left join op_amt_bins bins
        on claim_op >= bins.lb 
        and claim_op < bins.ub
)

select 
    bin_idx
    , '[' || cast(cast(bin_lb as int) as varchar) || ',' || cast(cast(bin_ub as int) as varchar) || ')' as bin_label
    , change_summary

    , count(claimer) as num_claimers
    , sum(claim_op) as claim_op

    , sum(vote_power_pre_ad) as vote_power_pre_ad
    , sum(vote_power_post_ad) as vote_power_post_ad
    , sum(vote_power_post_ad) - sum(vote_power_pre_ad) as vote_power_change
from claimer_vote_power_changes_with_claimop_bin -- claimer_vote_power_change.sql
group by 1,2,3
order by 1,3

