
with ai_tokens as (
    select   
        ait.name
        , ait.symbol
        , ait.address as token_address
        , tki.decimals
    from query_3486591 ait
    inner join tokens.erc20 tki
        on ait.address = tki.contract_address
        and ait.symbol = tki.symbol
    where tki.blockchain = 'ethereum'
)

select 
    tsf.*
from ai_tokens ait 
inner join erc20_ethereum.evt_Transfer tsf
    on ait.token_address = tsf.contract_address
where tsf.evt_block_time >= date '2018-04-30'
    and tsf."from" = 0x0000000000000000000000000000000000000000
limit 10