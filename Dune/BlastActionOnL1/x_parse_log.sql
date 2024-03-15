
with Event_ETHBridgeInitiated as (
    select 
        varbinary_ltrim(topic1) as from_address
        , bytearray_ltrim(topic2) as to_address -- bytearray_xxx与varbinary_xxx互为alias
        , varbinary_to_uint256(varbinary_substring(data,1,32)) as amount
    from ethereum.logs
    where contract_address = 0x3a05E5d33d7Ab3864D53aaEc93c8301C1Fa49115 -- Blast: L1 Bridge Proxy
        and topic0 = 0x2849b43074093a05396b6f2a937dee8565b15a48a7b3d4bffb732a5017380af5 -- ETHBridgeInitiated
        and block_date = date '2024-03-11' 
        and tx_hash = 0x877860dd5bb0912d23072e50b50ca07dc8233b8b3164d7b098212414cc89ec49
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
        and block_date = date '2024-03-11' 
        and tx_hash = 0xa09595ea792df62cd28749a97b7a53bb5ce7ed2e82e4b66fcab006278d81b6a9
        and varbinary_ltrim(topic2) = 0x -- 说明topic2全0，表明在L2 chain上存储的是native ETH
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
        and block_date = date '2024-03-11' 
        and tx_hash = 0xf94091a6c70989cf387e391a436be70f1ce4035fcddf4b887edf8ddf5cc00832
)

, Event_LidoYieldProvider_Staked as (
    select 
        topic1 as provider -- 实际上就是LidoYieldProvider的id
        , varbinary_to_uint256(varbinary_substring(data,1,32)) as amount
    from ethereum.logs
    where contract_address = 0x4316A00D31da1313617DbB04fD92F9fF8D1aF7Db -- Blast: Lido Yield Provider
        and topic0 = 0xad8699b31aa71e27625c441f641b4732d76e1b7475068543aaaee79bd2c3d1f6 -- Staked
        and block_date = date '2024-03-11' 
        and tx_hash = 0xa09595ea792df62cd28749a97b7a53bb5ce7ed2e82e4b66fcab006278d81b6a9
)

, Event_LidoYieldProvider_Unstaked as (
    select 
        topic1 as provider -- 实际上就是LidoYieldProvider的id
        , varbinary_to_uint256(varbinary_substring(data,1,32)) as amount
    from ethereum.logs
    where contract_address = 0x4316A00D31da1313617DbB04fD92F9fF8D1aF7Db -- Blast: Lido Yield Provider
        and topic0 = 0xeaf9a24a861b99687156137f32a9abfa90e5dd93a1eb9fb70cf4400e86b0839a -- Unstaked
        and block_date = date '2024-03-09' 
        and tx_hash = 0x6833d27a3d17cbb2de3a27350ddea66101eb9515bae2d0bc73cdd5c6d03ac406
)

, Event_LidoYieldProvider_Claimed as (
    select 
        topic1 as provider -- 实际上就是LidoYieldProvider的id
        , varbinary_to_uint256(varbinary_substring(data,1,32)) as claimedAmount
        , varbinary_to_uint256(varbinary_substring(data,1+32,32)) as expectedAmount
    from ethereum.logs
    where contract_address = 0x4316A00D31da1313617DbB04fD92F9fF8D1aF7Db -- Blast: Lido Yield Provider
        and topic0 = 0x41628d0ba42442e4aa4fc514eeb97bb7154969e70e6678229c836f3b9732ba90 -- Claimed
        and block_date = date '2024-03-10' 
        and tx_hash = 0xc71754b8c69becaf1cd5b35ecc07b4213c2b2a558b594f4f486c4ad8e991671e
)

select * from Event_LidoYieldProvider_Claimed