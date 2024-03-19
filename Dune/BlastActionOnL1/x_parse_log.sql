
--- ################################ deposit
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

--- ################################ ETH Manager 
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

, Event_ETHYieldManager_WithdrawRequested as (
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

, Event_ETHYieldManager_WithdrawClaimed as (
    select 
        varbinary_to_uint256(topic1) as requestId
        , bytearray_ltrim(topic2) as recipient 
        , varbinary_to_uint256(varbinary_substring(data,1,32)) as amount
    from ethereum.logs
    where contract_address = 0x98078db053902644191f93988341E31289E1C8FE -- Blast: ETH Yield Manager Proxy
        and topic0 = 0x8adb7a84b2998a8d11cd9284395f95d5a99f160be785ae79998c654979bd3d9a -- WithdrawalClaimed
        and block_date = date '2024-03-13' 
        and tx_hash = 0xbf125c91ce24cf9f98bd2302550f54b5c28590a98df870383910e8c59a39752f
)

, Event_ETHYieldManager_YieldReport as (
    select 
        varbinary_to_int256(varbinary_substring(data,1,32)) as yield -- not uint256, because yield can be negative
        , varbinary_to_uint256(varbinary_substring(data,1+32,32)) as insurancePremiumPaid
        , varbinary_to_uint256(varbinary_substring(data,1+2*32,32)) as insuranceWithdrawn
    from ethereum.logs
    where contract_address = 0x98078db053902644191f93988341E31289E1C8FE -- Blast: ETH Yield Manager Proxy
        and topic0 = 0x00de4b58e7863b1e3dce7259a138136239427388d53e4844f369cdee7a81dbf5 -- YieldReport
        and block_date = date '2024-03-11' 
        and tx_hash = 0xd55ff8f9eaf4867d126bfec77e5d7d15200d22565259f0b7e013b897ca02e92b
)

--- ################################ USD Manager 
, Event_DsrYieldProvider_Staked as (
    select 
        topic1 as provider -- 实际上就是DSR Yield Provider的id
        , varbinary_to_uint256(varbinary_substring(data,1,32)) as amount
    from ethereum.logs
    where contract_address = 0x0733F618118bF420b6b604c969498ecf143681a8 -- Blast: DSR Yield Provider
        and topic0 = 0xad8699b31aa71e27625c441f641b4732d76e1b7475068543aaaee79bd2c3d1f6 -- Staked
        and block_date = date '2024-03-12' 
        and tx_hash = 0x7bcd9f009a1fe9328b2222222b3331b3765c728901443b632796d6a9703d5c0a
)

, Event_DsrYieldProvider_Unstaked as (
    select 
        block_time 
        , tx_hash
        , varbinary_to_uint256(varbinary_substring(data,1,32)) as amount
    from ethereum.logs
    where contract_address = 0x0733F618118bF420b6b604c969498ecf143681a8 -- Blast: DSR Yield Provider
        and topic0 = 0xeaf9a24a861b99687156137f32a9abfa90e5dd93a1eb9fb70cf4400e86b0839a -- Unstaked
        and block_date >= date '2024-02-24' -- day when blast L1 bridge is deployed
)

, Event_DsrYieldProvider_Claimed as (
    -- example: https://etherscan.io/tx/0x11164c3009754aa49d3d59e5ae64e88eb6f95f87e0739417ab920a3aaecbe831#eventlog
    select
        block_time 
        , tx_hash
        -- , topic1 as provider -- 实际上就是DSR Yield Provider的id
        , varbinary_to_uint256(varbinary_substring(data,1,32)) as claimedAmount
        , varbinary_to_uint256(varbinary_substring(data,1+32,32)) as expectedAmount
    from ethereum.logs
    where contract_address = 0x0733F618118bF420b6b604c969498ecf143681a8 -- Blast: DSR Yield Provider
        and topic0 = 0x41628d0ba42442e4aa4fc514eeb97bb7154969e70e6678229c836f3b9732ba90 -- Claimed
        and block_date >= date '2024-02-24' -- day when blast L1 bridge is deployed
)

, Event_USDYieldManager_YieldReport as (
    select 
        varbinary_to_int256(varbinary_substring(data,1,32)) as yield -- not uint256, because yield can be negative
        , varbinary_to_uint256(varbinary_substring(data,1+32,32)) as insurancePremiumPaid
        , varbinary_to_uint256(varbinary_substring(data,1+2*32,32)) as insuranceWithdrawn
    from ethereum.logs
    where contract_address = 0xa230285d5683C74935aD14c446e137c8c8828438 -- Blast: USD Yield Manager Proxy
        and topic0 = 0x00de4b58e7863b1e3dce7259a138136239427388d53e4844f369cdee7a81dbf5 -- YieldReport
        and block_date = date '2024-03-13' 
        and tx_hash = 0x55766ee1cf72625691d694fdc32758efe75a2f1e1959e6d3c88d8554d794056f
)

, Event_OptimismPortalProxy_Withdraw_StableCoin as (
    --都是不成功的，L1只是接收到了withdraw请求，却没有接下来的动作了。所以目前还不支持withdraw stablecoin
    select *  
    from (
        select 
            block_time
            , tx_hash
            , varbinary_to_uint256(varbinary_substring(data,1,32)) as requestId
        from ethereum.logs
        where contract_address = 0x0Ec68c5B10F21EFFb74f2A5C61DFe6b08C0Db6Cb -- Blast: Optimism Portal Proxy
            and topic0 = 0x5d5446905f1f582d57d04ced5b1bed0f1a6847bcee57f7dd9d6f2ec12ab9ec2e --WithdrawalProven
            and block_date >= date '2024-02-24' -- day when blast L1 bridge is deployed
    )
    where requestId=0
)

, Event_DsrYieldManager_WithdrawRequested as (
    select 
        block_date
        , tx_hash
        , tx_from as user
        -- 不能用recipient,它们不是真正的收款人，而是Blast: Optimism Portal Proxy
        -- , bytearray_ltrim(topic2) as requestor 
        -- , bytearray_ltrim(topic3) as recipient 
        , varbinary_to_uint256(varbinary_substring(data,1,32)) as amount
    from ethereum.logs
    where contract_address = 0xa230285d5683C74935aD14c446e137c8c8828438 -- Blast: USD Yield Manager Proxy
        and topic0 = 0x00ae2c76ca218353c7995e13a4af773a35837cb6ebb8288092d8190bcd9c8f68 -- WithdrawalRequested
        and block_date >= date '2024-02-24' -- day when blast L1 bridge is deployed
)



select * from Event_DsrYieldManager_WithdrawRequested
order by block_time desc
