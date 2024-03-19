
with Event_UsdYieldManager_WithdrawRequested as (
    -- example: https://etherscan.io/tx/0xca79e3812dc6f8ef14fdd857c3f5a264cb632a237d8f038ae609a6593f92583d#eventlog
    select 
        block_time
        , tx_hash
        , tx_from as recipient
        , varbinary_to_uint256(topic1) as requestId
        -- 与ETHYieldManager_WithdrawRequested不同，这里的recipient倒是真正收款人，与tx_from相同
        -- , bytearray_ltrim(topic3) as recipient 
        , varbinary_to_uint256(varbinary_substring(data,1,32)) as amount
    from ethereum.logs
    where contract_address = 0xa230285d5683C74935aD14c446e137c8c8828438 -- Blast: USD Yield Manager Proxy
        and topic0 = 0x00ae2c76ca218353c7995e13a4af773a35837cb6ebb8288092d8190bcd9c8f68 -- WithdrawalRequested
        and block_date >= date '2024-02-24' -- day when blast L1 bridge is deployed
)

, Event_UsdYieldManager_WithdrawClaimed as (
    -- example: https://etherscan.io/tx/0x47d88f1c4ccf91437af0a61b6b75a4ef46d2d64a6540c8f48ce1a1fbe6a3fa01#eventlog
    select 
        block_time
        , tx_hash
        , varbinary_to_uint256(topic1) as requestId
        , bytearray_ltrim(topic2) as recipient 
        -- 虽然代码里写的参数名是amountOfETH，但是肯定这是DAI，不是ETH
        , varbinary_to_uint256(varbinary_substring(data,1,32)) as amount
    from ethereum.logs
    where contract_address = 0xa230285d5683C74935aD14c446e137c8c8828438 -- Blast: USD Yield Manager Proxy
        and topic0 = 0x8adb7a84b2998a8d11cd9284395f95d5a99f160be785ae79998c654979bd3d9a -- WithdrawalClaimed
        and block_date >= date '2024-02-24' -- day when blast L1 bridge is deployed
)

select   
    recipient
    , requestId
    , wr.block_time as request_time
    , wc.block_time as claim_time
    , get_href(get_chain_explorer_tx_hash('ethereum', wr.tx_hash), 'request tx') as request_tx
    , get_href(get_chain_explorer_tx_hash('ethereum', wc.tx_hash), 'claim tx') as claim_tx
    , cast(wr.amount as double)/1e18 as withdraw_dai
    , case when wc.amount = wr.amount then 'full withdraw' else 'partial withdraw' end as withdraw_flag
from Event_UsdYieldManager_WithdrawRequested wr 
left join Event_UsdYieldManager_WithdrawClaimed wc 
    using (recipient,requestId)
order by request_time