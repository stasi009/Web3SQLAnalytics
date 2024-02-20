
select 
    block_day,
    is_wash_trade,

    count(trade_tx_index) as total_num,
    sum(amount_usd) as total_volume,
    sum(taker_fee_amount_usd) as total_taker_fee,
    sum(maker_fee_amount_usd) as total_maker_fee,
    sum(royalty_fee_amount_usd) as total_royalty_fee
from (
    select 
        date_trunc('day',q.block_time) as block_day,
        q.*
    from query_3445248 q 
)  
group by 1,2