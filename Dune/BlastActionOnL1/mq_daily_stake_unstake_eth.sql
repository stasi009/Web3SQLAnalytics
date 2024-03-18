
with Event_ETHYieldManager_WithdrawRequested as (
    select 
        block_date
        , tx_hash
        , tx_from as user
        -- 不能用recipient,它们不是真正的收款人，而是Blast: Optimism Portal Proxy
        -- , bytearray_ltrim(topic2) as requestor 
        -- , bytearray_ltrim(topic3) as recipient 
        , varbinary_to_uint256(varbinary_substring(data,1,32)) as amount
    from ethereum.logs
    where contract_address = 0x98078db053902644191f93988341E31289E1C8FE -- Blast: ETH Yield Manager Proxy
        and topic0 = 0x00ae2c76ca218353c7995e13a4af773a35837cb6ebb8288092d8190bcd9c8f68 -- WithdrawalRequested
        and block_date >= date '2024-02-24' -- day when blast L1 bridge is deployed
)

, Event_ETHBridgeInitiated as (
    -- example: https://etherscan.io/tx/0x877860dd5bb0912d23072e50b50ca07dc8233b8b3164d7b098212414cc89ec49#eventlog
    select 
        block_date
        , tx_hash
        , varbinary_ltrim(topic1) as user
        , varbinary_to_uint256(varbinary_substring(data,1,32)) as amount
    from ethereum.logs
    where contract_address = 0x3a05E5d33d7Ab3864D53aaEc93c8301C1Fa49115 -- Blast: L1 Bridge Proxy
        and topic0 = 0x2849b43074093a05396b6f2a937dee8565b15a48a7b3d4bffb732a5017380af5 -- ETHBridgeInitiated
        and block_date >= date '2024-02-24' -- day when blast L1 bridge is deployed
)

, Event_ERC20BridgeInitiated_stETH as (
    -- example: https://etherscan.io/tx/0xa09595ea792df62cd28749a97b7a53bb5ce7ed2e82e4b66fcab006278d81b6a9#eventlog
    select 
        block_date 
        , tx_hash
        , varbinary_ltrim(topic3) as user
        , varbinary_to_uint256(varbinary_substring(data,1+32,32)) as amount
    from ethereum.logs
    where contract_address = 0x3a05E5d33d7Ab3864D53aaEc93c8301C1Fa49115 -- Blast: L1 Bridge Proxy
        and topic0 = 0x7ff126db8024424bbfd9826e8ab82ff59136289ea440b04b39a0df1b03b9cabf -- ERC20BridgeInitiated
        and varbinary_ltrim(topic1) = 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84 -- local token is stETH
        and varbinary_ltrim(topic2) = 0x -- 说明topic2全0，表明在L2 chain上存储的是native ETH
        and block_date >= date '2024-02-24' -- day when blast L1 bridge is deployed
)
