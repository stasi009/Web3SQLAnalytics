-- https://dune.com/queries/3593109

with claimer_delegate_changes as (
    select 
        *  
        , case 
            when vote_power_pre_ad <= 1e-6 and vote_power_post_ad > 1e-6
                then 'Begin Delegate'
            when vote_power_pre_ad > 1e-6 and vote_power_post_ad <= 1e-6
                then 'Quit Delegate'
            when vote_power_pre_ad <= 1e-6 and vote_power_post_ad <= 1e-6
                then 'Still Not Delegate'
            when delegate_before_airdrop = delegate_after_airdrop 
                then 'Keep Same Delegate'
            when delegate_before_airdrop <> delegate_after_airdrop 
                then 'Change Delegate'  
        end as change_after_airdrop
    from query_3598102
)

select 
    change_after_airdrop
    , count(claimer) as num_claimers
    , cast(count(claimer) as double) / (sum(count(claimer)) over () ) as claimer_percentage 
from claimer_delegate_changes
group by change_after_airdrop
order by num_claimers desc