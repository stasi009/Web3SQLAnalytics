-- https://dune.com/queries/3527835
with first_deposit_day as (
    select 
        sender 
        , min(block_date) as first_deposit_day
    from query_3527756 -- sq_all_deposits
    group by 1 -- 只根据daily来group，不涉及币种
)

, new_or_old_deposits as (
    select 
        de.block_date
        , de.sender 
        , case 
            when de.block_date = day1.first_deposit_day then 'new user'
            else 'old user'
        end as sender_new_old
    from query_3527756 de -- sq_all_deposits
    inner join first_deposit_day day1
        on de.sender = day1.sender
    where block_date >= date '{{start day}}'
)

select 
    block_date 
    , sender_new_old
    , approx_distinct(sender) as num_depositors
from new_or_old_deposits
group by 1,2