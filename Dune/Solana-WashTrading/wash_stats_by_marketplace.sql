
with group_stats_by_marketplace as (
    select 
        project || '-' || version as marketplace,

        count(trade_tx_index) as total_num,
        sum(is_wash_trade) as wash_num,

        sum(amount_usd) as total_volume,
        sum(amount_usd * is_wash_trade) as wash_volume,

        sum(taker_fee_amount_usd) as taker_fee,
        sum(taker_fee_amount_usd * is_wash_trade) as wash_taker_fee,

        sum(maker_fee_amount_usd) as maker_fee,
        sum(maker_fee_amount_usd * is_wash_trade) as wash_maker_fee,

        sum(royalty_fee_amount_usd) as royalty_fee,
        sum(royalty_fee_amount_usd * is_wash_trade) as wash_royalty_fee
    from query_3445248 q 
    group by 1
)

select 
    temp.*,
    
    total_num - wash_num as non_wash_num,
    cast(temp.wash_num as double) / temp.total_num as wash_num_percent,
    total_volume - wash_volume as non_wash_volume,
    temp.wash_volume / temp.total_volume as wash_vol_percent,

    temp.taker_fee / temp.total_volume as takerfee2vol_percent,
    temp.maker_fee / temp.total_volume as makerfee2vol_percent,

    temp.wash_taker_fee / temp.taker_fee as wash_takerfee_percent,
    temp.wash_maker_fee / temp.maker_fee as wash_makerfee_percent,
    temp.wash_royalty_fee / temp.royalty_fee as wash_royaltyfee_percent
from group_stats_by_marketplace temp
order by total_volume desc