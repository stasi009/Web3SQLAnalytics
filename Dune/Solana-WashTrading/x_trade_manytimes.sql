
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

bought_3x as (
    select 
        t1.buyer,
        t1.unique_nft_id,
        count(t1.trade_tx_index) as buy_times
    from latest_nft_trades t1 
    group by 1,2
    having count(t1.trade_tx_index) >= 3
),

sold_3x as (
    select 
        t1.seller,
        t1.unique_nft_id,
        count(t1.trade_tx_index) as buy_times
    from latest_nft_trades t1 
    group by 1,2
    having count(t1.trade_tx_index) >= 3
)

select * from bought_3x
order by buy_times desc
