
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

            trade_category,
            buyer,
            seller,

            amount_usd,
            taker_fee_amount_usd,
            maker_fee_amount_usd,
            royalty_fee_amount_usd
        from nft_solana.trades
    ) temp
    where temp.block_time >= now() - interval '{{backdays}}' day
        and date_trunc('day',temp.block_time) < current_date -- not include today's partial data
        and temp.trade_tx_index is not null
        and temp.unique_nft_id is not null
        and amount_usd >=1 -- limit dataset size
        and project = '{{market}}'
        and version = '{{market_version}}'
),

-- ! NOTE: very rare, nearly impossible
-- ! NOT join with this CTE to save some time
-- same_buyer_seller as (
--     select 
--         trade_tx_index,
--         true as same_buyer_seller
--     from latest_nft_trades t
--     where buyer = seller 
-- ),

-- ! NOTE: must add t1.trade_category = t2.trade_category
-- if A first sell it to B, which is a 'sell' trade
-- and then A buy it back from B, which is a 'buy' trade
-- in both trades, A is active trader (i.e., tx signer), 
-- sometimes it's normal, maybe A find some opportunity, and B is a professional dealer, whose job is just accepting all orders
-- what's making such cases abnormal is when A does it many times, which will be handled by CTE 'buy_same_manytimes' and 'sell_same_manytimes'

-- if A first sell it to B (A is the active trader, i.e., tx_signer), which is a 'sell' trade
-- and then B sell it to A (B is the active trader, i.e., tx_signer), which is a 'sell' trade
-- the reason A is willing to reverse his position passively in 2nd trading, is highly because A and B belongs to same person
back_forth_trade as (
    select 
        t1.trade_tx_index,
        true as back_forth_trade
    from latest_nft_trades t1
    inner join latest_nft_trades t2
        on t1.unique_nft_id = t2.unique_nft_id
        and t1.buyer = t2.seller
        and t1.seller = t2.buyer
        and t1.trade_category = t2.trade_category
    group by t1.trade_tx_index
),

buy_same_manytimes as (
    with buy_trades as (-- filter before join, reduce shuffle time
        select *
        from latest_nft_trades
        where trade_category = 'buy'
    )
    select 
        t1.trade_tx_index,
        true as buy_same_manytimes
    from buy_trades t1 
    inner join buy_trades t2 
        on t1.unique_nft_id = t2.unique_nft_id
        and t1.buyer = t2.buyer -- active trader (tx_signer) is buyer
    group by t1.trade_tx_index
    having count(t1.trade_tx_index) >= {{trade_same_nft_max_times}}
),

sell_same_manytimes as (
    with sell_trades as (-- filter before join, reduce shuffle time
        select *
        from latest_nft_trades
        where trade_category = 'sell'
    )
    select 
        t1.trade_tx_index,
        true as sell_same_manytimes
    from sell_trades t1 
    inner join sell_trades t2 
        on t1.unique_nft_id = t2.unique_nft_id
        and t1.seller = t2.seller -- active trader (tx_signer) is seller
    group by t1.trade_tx_index
    having count(t1.trade_tx_index) >= {{trade_same_nft_max_times}}
),

nft_trades_with_washflag as (
    select 
        lnt.*,
        cast(COALESCE(bft.back_forth_trade, false) as int) as back_forth_trade,
        cast(COALESCE(bsm.buy_same_manytimes,false) as int) as buy_same_manytimes,
        cast(COALESCE(ssm.sell_same_manytimes,false) as int) as sell_same_manytimes,
        -- if one is true, even the other is null, 'or' still return true
        -- ! NOTE: true or null return true, false or null return null
        cast(COALESCE(bft.back_forth_trade
                or bsm.buy_same_manytimes
                or ssm.sell_same_manytimes, false) as int) as is_wash_trade 
    from latest_nft_trades lnt 
    left join back_forth_trade bft 
        on bft.trade_tx_index = lnt.trade_tx_index
    left join buy_same_manytimes bsm
        on bsm.trade_tx_index = lnt.trade_tx_index
    left join sell_same_manytimes ssm
        on ssm.trade_tx_index = lnt.trade_tx_index
),

group_stats_by_day as (
    select 
        date_trunc('day',block_time) as block_day,

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
    from nft_trades_with_washflag q 
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
from group_stats_by_day temp
order by block_day


-- select * from "query_3449073(backdays='90',market='magiceden',market_version='mmm')"
-- select * from "query_3449073(backdays='90',market='magiceden',market_version='v2')"
-- select * from "query_3449073(backdays='90',market='tensorswap',market_version='v1')"
-- select * from "query_3449073(backdays='90',market='tensorswap',market_version='v2')"