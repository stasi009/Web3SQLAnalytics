select 
    is_wash_trade,
    case 
        when is_wash_trade=1 then 'wash'
        when is_wash_trade=0 then 'not wash'
    end as wash_flag,
    count(trade_tx_index) as total_num,
    sum(amount_usd) as total_volume,
    sum(taker_fee_amount_usd) as total_taker_fee,
    sum(maker_fee_amount_usd) as total_maker_fee,
    sum(royalty_fee_amount_usd) as total_royalty_fee
from query_3445248
group by is_wash_trade
order by is_wash_trade