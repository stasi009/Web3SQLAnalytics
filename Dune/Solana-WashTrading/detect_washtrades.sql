
with latest_nft_trades as (
    select 
        block_time,
        tx_id || '-'|| cast(outer_instruction_index as varchar) || '-' || cast(inner_instruction_index as varchar) as trade_tx_index,

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
    where block_time >= now() - interval '{{backdays}}' day
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
        and t1.trade_tx_index is not null
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
        and t1.trade_tx_index is not null
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
        and t1.trade_tx_index is not null
    group by t1.trade_tx_index
    having count(t1.trade_tx_index) >= 3
)

select 
    nt.buyer,
    nt.unique_nft_id,
    count(*) as count
from buy_same_3x b3
inner join latest_nft_trades nt
    on nt.trade_tx_index = b3.trade_tx_index
group by 1,2
order by count desc