-- https://dune.com/queries/3603698
with between_claimers_transfers as (
    select * from "query_3603670(backdays='365',blockchain='ethereum')" -- between_claimer_transfer
    union all
    select * from "query_3603670(backdays='365',blockchain='optimism')"
    union all
    select * from "query_3603670(backdays='365',blockchain='zora')"
    union all
    select * from "query_3603670(backdays='365',blockchain='base')"
)

select 
    account1 
    , account2 
    , count(1) as num_transfers
from between_claimers_transfers
group by 1,2 
order by num_transfers desc