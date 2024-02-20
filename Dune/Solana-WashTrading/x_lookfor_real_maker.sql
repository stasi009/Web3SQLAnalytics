
with all_trades as (
    select 
        DISTINCT t.trader
    from nft_solana.trades nst
    CROSS JOIN UNNEST(ARRAY[buyer, seller]) AS t(trader)
    where nst.block_time >= now() - interval '{{backdays}}' day
),

active_traders as (
    select DISTINCT tx_signer
    from nft_solana.trades nst
    where nst.block_time >= now() - interval '{{backdays}}' day
)

select  
    all.trader as dealer
from all_trades all 
left join active_traders signer 
    on all.trader = signer.tx_signer
where signer.tx_signer is null
