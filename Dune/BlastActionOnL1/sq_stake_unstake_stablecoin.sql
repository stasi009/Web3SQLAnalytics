with Event_UsdYieldManager_WithdrawRequested as (
    -- example: https://etherscan.io/tx/0xca79e3812dc6f8ef14fdd857c3f5a264cb632a237d8f038ae609a6593f92583d#eventlog
    select 
        block_date
        , 'unstake dai request' as action
        , tx_hash
        , tx_from as user
        -- 与ETHYieldManager_WithdrawRequested不同，这里的recipient倒是真正收款人，与tx_from相同
        -- , bytearray_ltrim(topic3) as recipient 
        , varbinary_to_uint256(varbinary_substring(data,1,32)) as amount -- dai或usdb的数量
    from ethereum.logs
    where contract_address = 0xa230285d5683C74935aD14c446e137c8c8828438 -- Blast: USD Yield Manager Proxy
        and topic0 = 0x00ae2c76ca218353c7995e13a4af773a35837cb6ebb8288092d8190bcd9c8f68 -- WithdrawalRequested
        and block_date >= date '2024-02-24' -- day when blast L1 bridge is deployed
)

, Event_ERC20BridgeInitiated_StableCoin as (
    -- example: https://etherscan.io/tx/0xf94091a6c70989cf387e391a436be70f1ce4035fcddf4b887edf8ddf5cc00832#eventlog
    select 
        log.block_date
        , 'stake stablecoin' as action
        , tx_hash
        , varbinary_ltrim(log.topic3) as user
        , varbinary_to_uint256(varbinary_substring(log.data,1+32,32)) as amount -- DAI and USDB both 18 decimals
    from ethereum.logs log
    where log.contract_address = 0x3a05E5d33d7Ab3864D53aaEc93c8301C1Fa49115 -- Blast: L1 Bridge Proxy
        and log.topic0 = 0x7ff126db8024424bbfd9826e8ab82ff59136289ea440b04b39a0df1b03b9cabf -- ERC20BridgeInitiated
        and varbinary_ltrim(log.topic2) = 0x4300000000000000000000000000000000000003 -- USDB on blast L2
        and log.block_date >= date '2024-02-24' -- day when blast L1 bridge is deployed
)


select * from Event_ERC20BridgeInitiated_StableCoin
union all 
select * from Event_UsdYieldManager_WithdrawRequested