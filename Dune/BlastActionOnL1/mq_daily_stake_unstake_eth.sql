
with Event_ETHYieldManager_WithdrawRequested as (
    select 
        varbinary_to_uint256(topic1) as requestId
        , bytearray_ltrim(topic2) as requestor 
        , bytearray_ltrim(topic3) as recipient 
        , varbinary_to_uint256(varbinary_substring(data,1,32)) as amount
    from ethereum.logs
    where contract_address = 0x98078db053902644191f93988341E31289E1C8FE -- Blast: ETH Yield Manager Proxy
        and topic0 = 0x00ae2c76ca218353c7995e13a4af773a35837cb6ebb8288092d8190bcd9c8f68 -- WithdrawalRequested
        and block_date = date '2024-03-01' 
        and tx_hash = 0x1884c39b7a9c598d92bf0ecd2e2636a33cd44dfb4186476901eeedceea5e9754
)