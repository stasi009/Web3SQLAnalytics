
with Event_ETHBridgeInitiated as (
    select 
        block_date
        , tx_hash 
        , 'ETH' as token
        , varbinary_ltrim(topic1) as sender
        , varbinary_to_uint256(varbinary_substring(data,1,32)) as amount
    from ethereum.logs
    where contract_address = 0x3a05E5d33d7Ab3864D53aaEc93c8301C1Fa49115 -- Blast: L1 Bridge Proxy
        and topic0 = 0x2849b43074093a05396b6f2a937dee8565b15a48a7b3d4bffb732a5017380af5 -- ETHBridgeInitiated
        and block_date between current_date - interval '{{back_days}}' day and current_date - interval '1' day
)

, Event_ERC20BridgeInitiated_stETH as (
    select 
        block_date 
        , tx_hash
        , 'stETH' as token
        , varbinary_ltrim(topic3) as sender
        , varbinary_to_uint256(varbinary_substring(data,1+32,32)) as amount
    from ethereum.logs
    where contract_address = 0x3a05E5d33d7Ab3864D53aaEc93c8301C1Fa49115 -- Blast: L1 Bridge Proxy
        and topic0 = 0x7ff126db8024424bbfd9826e8ab82ff59136289ea440b04b39a0df1b03b9cabf -- ERC20BridgeInitiated
        and varbinary_ltrim(topic1) = 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84 -- local token is stETH
        and varbinary_ltrim(topic2) = 0x -- 说明topic2全0，表明在L2 chain上存储的是native ETH
        and block_date between current_date - interval '{{back_days}}' day and current_date - interval '1' day
)

, Event_ERC20BridgeInitiated_StableCoin as (
    select 
        block_date
        , tx_hash
        , coalesce(tkinfo.symbol,'other stablecoin') as token 
        , varbinary_ltrim(topic3) as sender
        , varbinary_to_uint256(varbinary_substring(data,1+32,32)) as amount
    from ethereum.logs
    left join tokens.erc20 tkinfo
        on varbinary_ltrim(topic1) = tkinfo.contract_address -- match on local token info
    where contract_address = 0x3a05E5d33d7Ab3864D53aaEc93c8301C1Fa49115 -- Blast: L1 Bridge Proxy
        and topic0 = 0x7ff126db8024424bbfd9826e8ab82ff59136289ea440b04b39a0df1b03b9cabf -- ERC20BridgeInitiated
        and varbinary_ltrim(topic2) = 0x4300000000000000000000000000000000000003 -- USDB on blast L2
        and tkinfo.blockchain = 'ethereum'
        and block_date between current_date - interval '{{back_days}}' day and current_date - interval '1' day
)

, All_Deposit as (
    select * from Event_ETHBridgeInitiated
    union all 
    select * from Event_ERC20BridgeInitiated_stETH
    union all
    select * from Event_ERC20BridgeInitiated_StableCoin
)

select * from All_Deposit
limit 100