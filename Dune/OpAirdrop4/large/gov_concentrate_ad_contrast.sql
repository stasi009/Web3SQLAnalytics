-- https://dune.com/queries/3616641
-- airdrop前后，集中度变化对比
select 
    'before_airdrop' as period
    , *   
from  "query_3615639(mode='before_airdrop',topn_delegates='{{topn_delegates}}')"

union all 

select 
    'current' as period
    , *   
from  "query_3615639(mode='current',topn_delegates='{{topn_delegates}}')"