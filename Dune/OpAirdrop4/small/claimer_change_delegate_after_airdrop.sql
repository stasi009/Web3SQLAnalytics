-- https://dune.com/queries/3593109/6053317

with claimer_delegate_changes as (
    select 
        *  
        , case 
            when is_delegated_pre_ad = 0 and is_delegated_post_ad = 1
                then 'Begin Delegate'
            when is_delegated_pre_ad = 1 and is_delegated_post_ad = 0
                then 'Quit Delegate'
            when is_delegated_pre_ad = 0 and is_delegated_post_ad = 0
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