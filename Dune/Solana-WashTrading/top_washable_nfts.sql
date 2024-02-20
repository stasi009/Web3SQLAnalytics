with create_nft as (
    SELECT 
        call_tx_id,
        call_block_time,
        case 
            when account_mint is not null then 'mint_' || account_mint
            when account_merkle_tree is not null then 'merkle_' || account_merkle_tree || '_' ||cast(leaf_id as varchar)
        end as unique_nft_id,
        COALESCE(collection_mint, verified_creator) as collection_or_creator, --collection is best, verified_creator is second best
        version,
        token_symbol,
        trim(split(token_name,'#')[1]) as token_name
    FROM tokens_solana.nft
    --if it is null then we don't want it
    WHERE COALESCE(collection_mint, verified_creator) is not null 
        and token_symbol is not null
        and token_name is not null
),

group_wash_by_nft as (
    select 
        unique_nft_id,

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
    from query_3445248 q 
    group by 1
)

select 
    topn.unique_nft_id,
    topn.total_wash_volume,
    ct.collection_or_creator,
    ct.version,
    ct.token_symbol,
    ct.token_name
from top_wash_nfts topn 
left join create_nft ct
    on topn.unique_nft_id = ct.unique_nft_id
