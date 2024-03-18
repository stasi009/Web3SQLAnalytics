
with Event_LidoYieldProvider_Staked as (-- ETH Yield Manager Stake to Lido
    -- example: https://etherscan.io/tx/0xa09595ea792df62cd28749a97b7a53bb5ce7ed2e82e4b66fcab006278d81b6a9#eventlog
    select 
        block_date
        , varbinary_to_uint256(varbinary_substring(data,1,32)) as stakedAmount
    from ethereum.logs
    where contract_address = 0x4316A00D31da1313617DbB04fD92F9fF8D1aF7Db -- Blast: Lido Yield Provider
        and topic0 = 0xad8699b31aa71e27625c441f641b4732d76e1b7475068543aaaee79bd2c3d1f6 -- Staked
        and block_date >= date '2024-02-24' -- day when blast L1 bridge is deployed
)

, Event_LidoYieldProvider_Claimed as (-- ETH Yield Manager Claim withdraw from Lido
    -- example: https://etherscan.io/tx/0xc71754b8c69becaf1cd5b35ecc07b4213c2b2a558b594f4f486c4ad8e991671e#eventlog
    select 
        block_date
        , varbinary_to_uint256(varbinary_substring(data,1,32)) as claimedAmount
    from ethereum.logs
    where contract_address = 0x4316A00D31da1313617DbB04fD92F9fF8D1aF7Db -- Blast: Lido Yield Provider
        and topic0 = 0x41628d0ba42442e4aa4fc514eeb97bb7154969e70e6678229c836f3b9732ba90 -- Claimed
        and block_date >= date '2024-02-24' -- day when blast L1 bridge is deployed
)