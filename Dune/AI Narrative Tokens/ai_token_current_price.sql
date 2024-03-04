select 
    ait.name
    , get_href(get_chain_explorer_address('ethereum', ait.token_address),ait.symbol) as token_url
    , ait.launch_date
    , date_diff('day', ait.launch_date, p.minute) as live_days
    , p.price
    , ait.description
from query_3486591 ait
inner join prices.usd_latest p 
    on ait.token_address = p.contract_address
    and ait.symbol = p.symbol
where p.blockchain = 'ethereum'
order by ait.symbol