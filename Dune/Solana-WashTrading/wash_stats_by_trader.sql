
with trader_trades as (
    select 
        q.*,
        q.buyer as trader -- active trader
    from query_3445248 q 
    where q.trade_category = 'buy'

    union all 

    select 
        q.*,
        q.seller as trader -- active trader
    from query_3445248 q 
    where q.trade_category = 'sell'
),

group_stats_by_trader as (
    select 
        trader,

        count(trade_tx_index) as total_num,
        sum(is_wash_trade) as total_wash_num,

        sum(amount_usd) as total_volume,
        sum(amount_usd * is_wash_trade) as total_wash_volume,

        sum(taker_fee_amount_usd) as total_taker_fee,
        sum(taker_fee_amount_usd * is_wash_trade) as total_wash_taker_fee,

        sum(maker_fee_amount_usd) as total_maker_fee,
        sum(maker_fee_amount_usd * is_wash_trade) as total_wash_maker_fee,

        sum(royalty_fee_amount_usd) as total_royalty_fee,
        sum(royalty_fee_amount_usd * is_wash_trade) as total_wash_royalty_fee
    from trader_trades
    group by 1
)

select 
    temp.*,
    temp.total_taker_fee / temp.total_volume as takerfee2vol_percent,
    temp.total_maker_fee / temp.total_volume as makerfee2vol_percent,
    cast(temp.total_wash_num as double) / temp.total_num as wash_num_percent,
    temp.total_wash_volume / temp.total_volume as wash_vol_percent,
    temp.total_wash_taker_fee / temp.total_taker_fee as wash_takerfee_percent,
    temp.total_wash_maker_fee / temp.total_maker_fee as wash_makerfee_percent,
    temp.total_wash_royalty_fee / temp.total_royalty_fee as wash_royaltyfee_percent
from group_stats_by_trader temp
where total_volume >= 10000
order by total_wash_volume desc