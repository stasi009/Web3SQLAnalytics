
with Event_ETHBridgeInitiated as (
    select 
        varbinary_ltrim(topic1) as from_address
        , bytearray_ltrim(topic2) as to_address -- bytearray_xxx与varbinary_xxx互为alias
        , varbinary_to_uint256(varbinary_substring(data,1,32)) as amount
    from ethereum.logs
    where contract_address = 0x3a05E5d33d7Ab3864D53aaEc93c8301C1Fa49115 -- Blast: L1 Bridge Proxy
        and topic0 = 0x2849b43074093a05396b6f2a937dee8565b15a48a7b3d4bffb732a5017380af5 -- ETHBridgeInitiated
        and block_date between current_date - interval '{{back_days}}' day and current_date - interval '1' day
)

, Event_ERC20BridgeInitiated_stETH as (
    select 
        varbinary_ltrim(topic1) as localToken
        , varbinary_ltrim(topic2) as remoteToken
        , varbinary_ltrim(topic3) as from_address
        , varbinary_ltrim(varbinary_substring(data,1,32)) as to_address 
        , varbinary_to_uint256(varbinary_substring(data,1+32,32)) as amount
    from ethereum.logs
    where contract_address = 0x3a05E5d33d7Ab3864D53aaEc93c8301C1Fa49115 -- Blast: L1 Bridge Proxy
        and topic0 = 0x7ff126db8024424bbfd9826e8ab82ff59136289ea440b04b39a0df1b03b9cabf -- ERC20BridgeInitiated
        and varbinary_ltrim(topic2) = 0x -- 说明topic2全0，表明在L2 chain上存储的是native ETH
        and block_date between current_date - interval '{{back_days}}' day and current_date - interval '1' day
)

, Event_ERC20BridgeInitiated_StableCoin as (
    select 
        varbinary_ltrim(topic1) as localToken
        , varbinary_ltrim(topic2) as remoteToken
        , varbinary_ltrim(topic3) as from_address
        , varbinary_ltrim(varbinary_substring(data,1,32)) as to_address 
        , varbinary_to_uint256(varbinary_substring(data,1+32,32)) as amount
    from ethereum.logs
    where contract_address = 0x3a05E5d33d7Ab3864D53aaEc93c8301C1Fa49115 -- Blast: L1 Bridge Proxy
        and topic0 = 0x7ff126db8024424bbfd9826e8ab82ff59136289ea440b04b39a0df1b03b9cabf -- ERC20BridgeInitiated
        and block_date between current_date - interval '{{back_days}}' day and current_date - interval '1' day
)