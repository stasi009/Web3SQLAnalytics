
with latest_nft_trades as (
    select 
        temp.* 
    from (
        select 
            block_time,
            tx_id || '-'|| cast(outer_instruction_index as varchar) || '-' || cast(COALESCE(inner_instruction_index,0) as varchar) as trade_tx_index,

            case 
                when account_mint is not null then 'mint_' || account_mint
                when account_merkle_tree is not null then 'merkle_' || account_merkle_tree || '_' ||cast(leaf_id as varchar)
            end as unique_nft_id,

            project,
            version,

            buyer,
            seller,

            amount_usd,
            taker_fee_amount_usd,
            maker_fee_amount_usd,
            royalty_fee_amount_usd
        from nft_solana.trades
    ) temp
    where temp.block_time >= now() - interval '{{backdays}}' day
        and temp.trade_tx_index is not null
        and temp.unique_nft_id is not null
        and amount_usd >=1 -- limit dataset size
),

same_buyer_seller as (
    select 
        trade_tx_index,
        true as same_buyer_seller
    from latest_nft_trades t
    where buyer = seller 
),

back_forth_trade as (
    select 
        t1.trade_tx_index,
        true as back_forth_trade
    from latest_nft_trades t1
    inner join latest_nft_trades t2
        on t1.unique_nft_id = t2.unique_nft_id
        and t1.buyer = t2.seller
        and t1.seller = t2.buyer
    group by t1.trade_tx_index
),

buy_same_3x as (
    select 
        t1.trade_tx_index,
        true as buy_same_3x
    from latest_nft_trades t1 
    inner join latest_nft_trades t2 
        on t1.unique_nft_id = t2.unique_nft_id
        and t1.buyer = t2.buyer
    group by t1.trade_tx_index
    having count(t1.trade_tx_index) >= 3
),

sell_same_3x as (
    select 
        t1.trade_tx_index,
        true as sell_same_3x
    from latest_nft_trades t1 
    inner join latest_nft_trades t2 
        on t1.unique_nft_id = t2.unique_nft_id
        and t1.seller = t2.seller
    group by t1.trade_tx_index
    having count(t1.trade_tx_index) >= 3
),

wash_extend_trades as (
    select 
        lnt.*,
        cast(COALESCE(sbs.same_buyer_seller,false) as int) as same_buyer_seller,
        cast(COALESCE(bft.back_forth_trade, false) as int) as back_forth_trade,
        cast(COALESCE(bs3.buy_same_3x,false) as int) as buy_same_3x,
        cast(COALESCE(ss3.sell_same_3x,false) as int) as sell_same_3x,
        cast(COALESCE(sbs.same_buyer_seller
                or bft.back_forth_trade
                or bs3.buy_same_3x
                or ss3.sell_same_3x, false) as int) as is_wash_trade 
    from latest_nft_trades lnt 
    left join same_buyer_seller sbs 
        on sbs.trade_tx_index = lnt.trade_tx_index
    left join back_forth_trade bft 
        on bft.trade_tx_index = lnt.trade_tx_index
    left join buy_same_3x bs3 
        on bs3.trade_tx_index = lnt.trade_tx_index
    left join sell_same_3x ss3 
        on ss3.trade_tx_index = lnt.trade_tx_index
)


select 
    temp.*,
    cast(temp.total_wash_num as double) / temp.total_num as wash_num_percent,
    temp.total_wash_volume / temp.total_volume as wash_vol_percent,
    temp.total_wash_taker_fee / temp.total_taker_fee as wash_takerfee_percent,
    temp.total_wash_maker_fee / temp.total_maker_fee as wash_makerfee_percent,
    temp.total_wash_royalty_fee / temp.total_royalty_fee as wash_royaltyfee_percent
from (
    select 
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
    from wash_extend_trades
) temp
