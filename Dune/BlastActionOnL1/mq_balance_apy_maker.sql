with daily_stake_to_maker as (
    -- example: https://etherscan.io/tx/0x7bcd9f009a1fe9328b2222222b3331b3765c728901443b632796d6a9703d5c0a#eventlog
    select 
        block_date 
        , sum(varbinary_to_uint256(varbinary_substring(data,1,32))/1e18) as daily_stake
    from ethereum.logs
    where contract_address = 0x0733F618118bF420b6b604c969498ecf143681a8 -- Blast: DSR Yield Provider
        and topic0 = 0xad8699b31aa71e27625c441f641b4732d76e1b7475068543aaaee79bd2c3d1f6 -- Staked
        and block_date >= date '2024-02-24' -- day when blast L1 bridge is deployed
    group by 1
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
    group by 1
)

, daily_dai_yield as (
    -- example: https://etherscan.io/tx/0x55766ee1cf72625691d694fdc32758efe75a2f1e1959e6d3c88d8554d794056f#eventlog
    select 
        block_date
        , sum(varbinary_to_int256(varbinary_substring(data,1,32))/1e18) as daily_yield -- not uint256, because yield can be negative
    from ethereum.logs
    where contract_address = 0xa230285d5683C74935aD14c446e137c8c8828438 -- Blast: USD Yield Manager Proxy
        and topic0 = 0x00de4b58e7863b1e3dce7259a138136239427388d53e4844f369cdee7a81dbf5 -- YieldReport
        and block_date >= date '2024-02-24' -- day when blast L1 bridge is deployed
    group by 1
)

select 
    *
    , case 
        when maker_balance=0 then 0 
        else cast(daily_yield as double) * 365.0/ maker_balance
    end as yield_apy
from (
    select 
        block_date
        , coalesce(daily_stake,0) as daily_stake
        , -1*coalesce(daily_claim,0) as daily_claim
        , coalesce(daily_yield,0) as daily_yield
        -- maker_balance: DSR Yield Manager's balance in Maker
        , sum(coalesce(daily_stake,0) - coalesce(daily_claim,0)) over (order by block_date) maker_balance
    -- sequence includes both ends
    -- start day is when blast L1 bridge is deployed
    from unnest(sequence(date '2024-02-24', current_date - interval '1' day, interval '1' day)) as days(block_date)
    left join daily_stake_to_maker stake using (block_date)
    left join daily_claim_from_maker claim using (block_date)
    left join daily_dai_yield yield using (block_date)
)
