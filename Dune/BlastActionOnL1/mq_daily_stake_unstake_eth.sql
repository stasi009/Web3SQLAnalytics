
with Event_ETHYieldManager_WithdrawRequested as (
    select 
        block_date
        , tx_hash
        , tx_from as withdraw_user
        -- 不能用recipient,它们不是真正的收款人，而是Blast: Optimism Portal Proxy
        -- , bytearray_ltrim(topic2) as requestor 
        -- , bytearray_ltrim(topic3) as recipient 
        , varbinary_to_uint256(varbinary_substring(data,1,32))/1e18 as amount
    from ethereum.logs
    where contract_address = 0x98078db053902644191f93988341E31289E1C8FE -- Blast: ETH Yield Manager Proxy
        and topic0 = 0x00ae2c76ca218353c7995e13a4af773a35837cb6ebb8288092d8190bcd9c8f68 -- WithdrawalRequested
        and block_date >= date '2024-02-24' -- day when blast L1 bridge is deployed
)

, daily_withdraw_request as (
    select 
        block_date 
    from Event_ETHYieldManager_WithdrawRequested


)

