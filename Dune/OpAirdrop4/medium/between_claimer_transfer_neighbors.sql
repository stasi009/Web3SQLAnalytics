with between_claimers_transfers as (
    select * from "query_3603670(backdays='365',blockchain='ethereum')" -- between_claimer_transfer
    union all
    select * from "query_3603670(backdays='365',blockchain='optimism')"
    union all
    select * from "query_3603670(backdays='365',blockchain='zora')"
    union all
    select * from "query_3603670(backdays='365',blockchain='base')"
)

, pairs as (
    select 
        account1 
        , account2 
        , count(1) as num_transfers
    from between_claimers_transfers
    group by 1,2 
    having count(1) > 3
)

select 
    center 
    , count(neighbor) as num_neighbors
from (
    select account1 as center, account2 as neighbor from pairs 
    union all 
    select account2 as center, account1 as neighbor from pairs
)
group by 1 
order by num_neighbors desc




