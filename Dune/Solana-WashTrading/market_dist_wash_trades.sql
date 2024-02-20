
select 
    date_trunc('day',block_time) as block_day,
    project || '-' || version as marketplace,

    count(trade_tx_index) as total_num,
    sum(amount_usd) as total_volume,
    sum(taker_fee_amount_usd) as total_taker_fee,
    sum(maker_fee_amount_usd) as total_maker_fee,
    sum(royalty_fee_amount_usd) as total_royalty_fee
from "query_3445248(backdays='90')" q 
where is_wash_trade = 1
group by 1,2

