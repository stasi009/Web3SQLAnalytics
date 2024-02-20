with nfts_summary as (
    SELECT 
        COALESCE(collection_mint, verified_creator) as collection_or_creator --collection is best, verified_creator is second best
        , version
        , array_agg(distinct token_symbol) filter (where trim(token_symbol) != '' AND token_symbol not like '%#%' AND token_symbol is not null) as symbols
        , array_agg(distinct trim(split(token_name,'#')[1])) filter (where trim(token_name) is not null) as tokens
        , count(*) as supply
    FROM tokens_solana.nft
    WHERE COALESCE(collection_mint, verified_creator) is not null --if it is null then we don't want it
    -- AND (collection_mint != '5PA96eCFHJSFPY9SWFeRJUHrpoNF5XZL6RrE1JADXhxf' 
    --     OR (collection_mint = '5PA96eCFHJSFPY9SWFeRJUHrpoNF5XZL6RrE1JADXhxf' AND version = 'cNFT')
    --     )
    group by 1,2
)