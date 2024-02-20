-- with create_nft as (
--     SELECT 
--         call_tx_id,
--         call_block_time,
--         case 
--             when account_mint is not null then 'mint_' || account_mint
--             when account_merkle_tree is not null then 'merkle_' || account_merkle_tree || '_' ||cast(leaf_id as varchar)
--         end as unique_nft_id,
--         COALESCE(collection_mint, verified_creator) as collection_or_creator, --collection is best, verified_creator is second best
--         version,
--         token_symbol,
--         trim(split(token_name,'#')[1]) as token_name
--     FROM tokens_solana.nft
--     --if it is null then we don't want it
--     WHERE COALESCE(collection_mint, verified_creator) is not null 
--         and token_symbol is not null
--         and token_name is not null
-- ),

with group_wash_by_nft as (
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
    order by total_wash_volume desc
    limit 100
),

nft_with_link as (
    select 
        case when nft_infos[1] = 'mint' 
            then get_href(get_chain_explorer_address('solana', nft_infos[2] ),'mint_account') 
        end as mint_account,
        case when nft_infos[1] = 'merkle' 
            then get_href(get_chain_explorer_address('solana', nft_infos[2] ),'merkle_leaf' || nft_infos[3]) 
        end as merkle_tree_leaf,
        tmp.*
    from (
        select 
            n.*,
            split(unique_nft_id,'_') as nft_infos
        from group_wash_by_nft n
    ) tmp
)

select 
    t.mint_account,
    t.merkle_tree_leaf,

    t.total_num,
    t.total_wash_num,
    cast(t.total_wash_num as double) / t.total_num as wash_num_percent,

    t.total_volume,
    t.total_wash_volume,
    t.total_wash_volume / t.total_volume as wash_vol_percent

from nft_with_link t
order by total_wash_volume desc

