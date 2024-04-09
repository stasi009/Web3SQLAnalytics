-- https://dune.com/queries/3603670
with eth_transfer as (
    select 
        "from"
        , to  
        , 'eth' as transfer_asset
    from {{blockchain}}.transactions
    where block_time >= current_date - interval '{{backdays}}' day 
        and success
        and value > 0
)

, erc20_transfer as (
    select 
        "from"
        , to  
        , 'erc20' as transfer_asset
    from erc20_{{blockchain}}.evt_transfer
    where evt_block_time >= current_date - interval '{{backdays}}' day 
)

, nft_transfer as (
    select   
        "from"
        , to
        , 'nft' as transfer_asset
    from nft.transfers
    where block_time >= current_date - interval '{{backdays}}' day 
        and blockchain = '{{blockchain}}'
)

select 
    '{{blockchain}}' as blockchain
    , transfer_asset
    , if("from" < to, "from", to) as account1
    , if("from" < to, to, "from") as account2
from (
    select * from eth_transfer
    union all 
    select * from erc20_transfer 
    union all 
    select * from nft_transfer
)
inner join optimism_airdrop_4_optimism.MerkleDistributor_evt_Claimed as ac1
    on "from" = ac1.account  
inner join optimism_airdrop_4_optimism.MerkleDistributor_evt_Claimed as ac2
    on to = ac2.account
