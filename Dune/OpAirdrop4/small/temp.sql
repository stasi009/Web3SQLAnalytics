select
    evt_block_time as block_time 
    , evt_tx_hash
    , "from"
    , to
    , if(to = 0xe60ead0c469e9801c886ed1080524fe9ab76bdff,1,-1) * cast(tf.value as double)/1e18 as op_amount
from erc20_optimism.evt_transfer tf
where contract_address = 0x4200000000000000000000000000000000000042 -- OP Token
    and ("from" = 0xe60ead0c469e9801c886ed1080524fe9ab76bdff or to = 0xe60ead0c469e9801c886ed1080524fe9ab76bdff)
order by 1