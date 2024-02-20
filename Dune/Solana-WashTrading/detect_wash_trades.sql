
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
)

-- select 
--     lnt.*,
--     cast(COALESCE(sbs.same_buyer_seller,false) as int) as same_buyer_seller,
--     cast(COALESCE(bft.back_forth_trade, false) as int) as back_forth_trade,
--     cast(COALESCE(bsm.buy_same_manytimes,false) as int) as buy_same_manytimes,
--     cast(COALESCE(ssm.sell_same_manytimes,false) as int) as sell_same_manytimes,
--     -- if one is true, even the other is null, 'or' still return true
--     -- ! NOTE: true or null return true, false or null return null
--     cast(COALESCE(sbs.same_buyer_seller
--             or bft.back_forth_trade
--             or bsm.buy_same_manytimes
--             or ssm.sell_same_manytimes, false) as int) as is_wash_trade 
-- from latest_nft_trades lnt 
-- left join same_buyer_seller sbs 
--     on sbs.trade_tx_index = lnt.trade_tx_index
-- left join back_forth_trade bft 
--     on bft.trade_tx_index = lnt.trade_tx_index
-- left join buy_same_manytimes bsm
--     on bsm.trade_tx_index = lnt.trade_tx_index
-- left join sell_same_manytimes ssm
--     on ssm.trade_tx_index = lnt.trade_tx_index

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