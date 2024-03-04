select
    tsf.evt_block_time as block_time
    , tsf.contract_address as token_address
    , ait.name as token_name
    , tsf."from"
    , tsf.to
    , tsf.value / power(10, ait.decimals) as value_adjdec -- adjdec means "decimals adjusted"
from erc20_ethereum.evt_Transfer tsf 
inner join query_3486591 ait 
    on tsf.contract_address = ait.token_address -- only care about ai tokens
    and tsf.evt_block_time >= ait.launch_date -- redundant condition, but can speed up to add constraints on time