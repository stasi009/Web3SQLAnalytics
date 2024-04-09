with all_transfers as (
    select * from "query_3603670(backdays='365',blockchain='ethereum')"
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
from all_transfers
group by 1,2 
order by num_transfers desc