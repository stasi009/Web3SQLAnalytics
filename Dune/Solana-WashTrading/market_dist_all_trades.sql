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

            project || '-' || version as marketplace,

            trade_category,
            buyer,
            seller,

            amount_usd,
            taker_fee_amount_usd,
            maker_fee_amount_usd,
            royalty_fee_amount_usd
        from nft_solana.trades
    ) temp
    where temp.block_time >= now() - interval '90' day
        and date_trunc('day',temp.block_time) < current_date -- not include today's partial data
        and temp.trade_tx_index is not null
        and temp.unique_nft_id is not null
        and amount_usd >=1 -- limit dataset size
)

select 
    date_trunc('day',block_time) as block_day,
    marketplace,

    count(trade_tx_index) as total_num,
    sum(amount_usd) as total_volume,
    sum(taker_fee_amount_usd) as total_taker_fee,
    sum(maker_fee_amount_usd) as total_maker_fee,
    sum(royalty_fee_amount_usd) as total_royalty_fee
from latest_nft_trades
group by 1,2