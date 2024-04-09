-- https://dune.com/queries/3603775
with pairs as (
    select 
        account1 
        , account2 
        , num_transfers
    from query_3603698 -- between_claimer_transfer_pairs.sql, https://dune.com/queries/3603698
    where num_transfers > 3
)

select 
    center 
    , count(neighbor) as num_neighbors
    , sum(num_transfers) as total_transfers
from (
    select 
        account1 as center
        , account2 as neighbor 
        , num_transfers
    from pairs

    union all 
    
    select 
        account2 as center
        , account1 as neighbor 
        , num_transfers
    from pairs
)
group by 1 
order by total_transfers desc




