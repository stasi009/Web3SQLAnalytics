
with latest_nft_trades as (
    select 
        block_time,
        tx_id || cast(outer_instruction_index as varchar) || cast(inner_instruction_index as varchar) as unique_tradeid,

        case 
            when account_mint is not null then 'mint_' || account_mint
            when account_merkle_tree is not null then 'merkle_' || account_merkle_tree || '_' ||cast(leaf_id as varchar)
        end as unique_nft_id,

        project,
        version,
        project_program_id,

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
        unique_tradeid,
        'same_buyer_seller' as wash_type
    from latest_nft_trades t
    where buyer = seller 
),

back_forth_trade as (
    select 
        t1.unique_nft_id,
        
        t1.block_time as t1_blocktime,
        t1.tx_id as t1_tx,
        t1.buyer as t1_buyer,
        t1.seller as t1_seller,
        t1.amount_usd as t1_usd,

        t2.block_time as t2_bl2cktime,
        t2.tx_id as t2_tx,
        t2.buyer as t2_buyer,
        t2.seller as t2_seller,
        t2.amount_usd as t2_usd
    from latest_nft_trades t1
    inner join latest_nft_trades t2
    on t1.unique_nft_id = t2.unique_nft_id
    and t1.buyer = t2.seller
    and t1.seller = t2.buyer
    and t2.block_time > t1.block_time
)
select * from back_forth_trade
order by unique_nft_id, t1_blocktime
