with daily_stake_to_maker as (
    -- example: https://etherscan.io/tx/0x7bcd9f009a1fe9328b2222222b3331b3765c728901443b632796d6a9703d5c0a#eventlog
    select 
        block_date 
        , sum(varbinary_to_uint256(varbinary_substring(data,1,32))/1e18) as daily_stake
    from ethereum.logs
    where contract_address = 0x0733F618118bF420b6b604c969498ecf143681a8 -- Blast: DSR Yield Provider
        and topic0 = 0xad8699b31aa71e27625c441f641b4732d76e1b7475068543aaaee79bd2c3d1f6 -- Staked
        and block_date >= date '2024-02-24' -- day when blast L1 bridge is deployed
)

, daily_claim_from_maker as (
    -- example: https://etherscan.io/tx/0x11164c3009754aa49d3d59e5ae64e88eb6f95f87e0739417ab920a3aaecbe831#eventlog
    select
        block_date
        , sum(varbinary_to_uint256(varbinary_substring(data,1,32))/1e18) as daily_claim
    from ethereum.logs
    where contract_address = 0x0733F618118bF420b6b604c969498ecf143681a8 -- Blast: DSR Yield Provider
        and topic0 = 0x41628d0ba42442e4aa4fc514eeb97bb7154969e70e6678229c836f3b9732ba90 -- Claimed
        and block_date >= date '2024-02-24' -- day when blast L1 bridge is deployed
)
