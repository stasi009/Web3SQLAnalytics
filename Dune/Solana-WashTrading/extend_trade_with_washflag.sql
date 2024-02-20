
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
        and temp.trade_tx_index is not null
        and temp.unique_nft_id is not null
        and amount_usd >=1 -- limit dataset size
),

-- ! NOTE: impossible, because one of the seller/buyer is the dealer
-- ! in Maket Making mechanism, impossible to trade with himself
-- same_buyer_seller as (
--     select 
--         trade_tx_index,
--         true as same_buyer_seller
--     from latest_nft_trades t
--     where buyer = seller 
-- ),

-- ! NOTE: because one of the seller/buyer is the dealer
-- ! so it's very normal for: A first buy from Dealer, and when price rises, A sell it back to Dealer
-- ! what's real abnormal is A does it many many times
-- back_forth_trade as (
--     select 
--         t1.trade_tx_index,
--         true as back_forth_trade
--     from latest_nft_trades t1
--     inner join latest_nft_trades t2
--         on t1.unique_nft_id = t2.unique_nft_id
--         and t1.buyer = t2.seller
--         and t1.seller = t2.buyer
--     group by t1.trade_tx_index
-- ),

buy_same_manytimes as (
    select 
        t1.trade_tx_index,
        true as buy_same_manytimes
    from latest_nft_trades t1 
    inner join latest_nft_trades t2 
        on t1.unique_nft_id = t2.unique_nft_id
        and t1.buyer = t2.buyer 
        and t1.trade_category = 'buy', -- active trader (tx_signer) is buyer
        and t2.trade_category = 'buy'
    group by t1.trade_tx_index
    having count(t1.trade_tx_index) >= {{trade_same_nft_max_times}}
),

sell_same_manytimes as (
    select 
        t1.trade_tx_index,
        true as sell_same_manytimes
    from latest_nft_trades t1 
    inner join latest_nft_trades t2 
        on t1.unique_nft_id = t2.unique_nft_id
        and t1.seller = t2.seller
        and t1.trade_category = 'sell' -- active trader (tx_signer) is seller
        and t2.trade_category = 'sell'
    group by t1.trade_tx_index
    having count(t1.trade_tx_index) >= {{trade_same_nft_max_times}}
)

select 
    lnt.*,
    cast(
        -- if one is true, even the other is null, 'or' still return true
        -- ! NOTE: true or null return true, false or null return null
        COALESCE(bsm.buy_same_manytimes or ssm.sell_same_manytimes, false) 
    as int) as is_wash_trade 
from latest_nft_trades lnt 
left join buy_same_manytimes bsm
    on bsm.trade_tx_index = lnt.trade_tx_index
left join sell_same_manytimes ssm
    on ssm.trade_tx_index = lnt.trade_tx_index