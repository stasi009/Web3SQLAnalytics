

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
where block_date = date '2024-02-18'
    and (buyer = 'A5v6o2GjHXRzMK8PXB4fxGpBYBzqJnvEo51LvfF1vAf9'
        or seller = 'A5v6o2GjHXRzMK8PXB4fxGpBYBzqJnvEo51LvfF1vAf9')
order by block_time

