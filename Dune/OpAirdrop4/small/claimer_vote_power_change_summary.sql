-- https://dune.com/queries/3598528
select 
    change_summary
    , count(claimer) as num_claimers
    , sum(claim_op) as claim_op

    , sum(vote_power_pre_ad) as vote_power_pre_ad
    , sum(vote_power_post_ad) as vote_power_post_ad
    , sum(vote_power_post_ad) - sum(vote_power_pre_ad) as vote_power_change
from query_3598102 -- claimer_vote_power_change.sql
group by change_summary
order by num_claimers desc

