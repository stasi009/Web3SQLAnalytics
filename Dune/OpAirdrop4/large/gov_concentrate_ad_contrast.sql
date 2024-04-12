
select 
    'before_airdrop' as period
    , *   
from  "query_3615639(mode='before_airdrop',topn_delegates='100')"

union all 

select 
    'current' as period
    , *   
from  "query_3615639(mode='current',topn_delegates='100')"