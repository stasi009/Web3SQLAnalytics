
with Event_LidoYieldProvider_Staked as (
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