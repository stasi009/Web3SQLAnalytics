SELECT 
    call_tx_id,
    call_block_time,
    case 
        when account_mint is not null then 'mint_' || account_mint
        when account_merkle_tree is not null then 'merkle_' || account_merkle_tree || '_' ||cast(leaf_id as varchar)
    end as unique_nft_id,
    COALESCE(collection_mint, verified_creator) as collection_or_creator, --collection is best, verified_creator is second best
    version
FROM tokens_solana.nft
WHERE COALESCE(collection_mint, verified_creator) is not null --if it is null then we don't want it
    and call_block_time >= now() - interval '90' day