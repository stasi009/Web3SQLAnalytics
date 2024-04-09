
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

select * from eth_transfer
union all 
select * from erc20_transfer 
union all 
select * from nft_transfer