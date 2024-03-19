-- https://dune.com/queries/3527756
with Event_ETHBridgeInitiated as (
    -- example: https://etherscan.io/tx/0x877860dd5bb0912d23072e50b50ca07dc8233b8b3164d7b098212414cc89ec49#eventlog
    select 
        block_date
        , tx_hash 
        , 'ETH' as token
        , 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2 as price_token -- use WETH to query price
        -- 不能用varbinary_ltrim(topic1) as sender，最多可能是Blast: Deposit
        , tx_from as sender
        , varbinary_to_uint256(varbinary_substring(data,1,32))/1e18 as amount
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
        , 'stETH' as token
        , 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84 as price_token -- use stETH to query price
        -- 不能用varbinary_ltrim(topic3) as sender，最多可能是Blast: Deposit
        , tx_from as sender
        , varbinary_to_uint256(varbinary_substring(data,1+32,32))/1e18 as amount
    from ethereum.logs
    where contract_address = 0x3a05E5d33d7Ab3864D53aaEc93c8301C1Fa49115 -- Blast: L1 Bridge Proxy
        and topic0 = 0x7ff126db8024424bbfd9826e8ab82ff59136289ea440b04b39a0df1b03b9cabf -- ERC20BridgeInitiated
        and varbinary_ltrim(topic1) = 0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84 -- local token is stETH
        and varbinary_ltrim(topic2) = 0x -- 说明topic2全0，表明在L2 chain上存储的是native ETH
        and block_date >= date '2024-02-24' -- day when blast L1 bridge is deployed
)

, Event_ERC20BridgeInitiated_StableCoin as (
    -- example: https://etherscan.io/tx/0xf94091a6c70989cf387e391a436be70f1ce4035fcddf4b887edf8ddf5cc00832#eventlog
    select 
        log.block_date
        , log.tx_hash
        , coalesce(tkinfo.symbol,'other stablecoin') as token 
        , tkinfo.contract_address as price_token
        -- 不能用varbinary_ltrim(log.topic3) as sender，最多可能是Blast: Deposit
        , tx_from as sender
        , varbinary_to_uint256(varbinary_substring(log.data,1+32,32))/1e18 as amount -- DAI and USDB both 18 decimals
    from ethereum.logs log
    left join tokens.erc20 tkinfo
        on varbinary_ltrim(log.topic1) = tkinfo.contract_address -- match on local token address
    where log.contract_address = 0x3a05E5d33d7Ab3864D53aaEc93c8301C1Fa49115 -- Blast: L1 Bridge Proxy
        and log.topic0 = 0x7ff126db8024424bbfd9826e8ab82ff59136289ea440b04b39a0df1b03b9cabf -- ERC20BridgeInitiated
        and varbinary_ltrim(log.topic2) = 0x4300000000000000000000000000000000000003 -- USDB on blast L2
        and tkinfo.blockchain = 'ethereum'
        and log.block_date >= date '2024-02-24' -- day when blast L1 bridge is deployed
)

select * from Event_ETHBridgeInitiated
union all 
select * from Event_ERC20BridgeInitiated_stETH
union all
select * from Event_ERC20BridgeInitiated_StableCoin