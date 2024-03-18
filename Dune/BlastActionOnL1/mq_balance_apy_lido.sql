
with daily_staked_to_lido as (-- ETH Yield Manager Stake to Lido
    -- example: https://etherscan.io/tx/0xa09595ea792df62cd28749a97b7a53bb5ce7ed2e82e4b66fcab006278d81b6a9#eventlog
    select 
        block_date
        , sum(varbinary_to_uint256(varbinary_substring(data,1,32))/1e18) as daily_stake
    from ethereum.logs
    where contract_address = 0x4316A00D31da1313617DbB04fD92F9fF8D1aF7Db -- Blast: Lido Yield Provider
        and topic0 = 0xad8699b31aa71e27625c441f641b4732d76e1b7475068543aaaee79bd2c3d1f6 -- Staked
        and block_date >= date '2024-02-24' -- day when blast L1 bridge is deployed
    group by 1
)

, daily_claim_from_lido as (-- ETH Yield Manager Claim withdraw from Lido
    -- example: https://etherscan.io/tx/0xc71754b8c69becaf1cd5b35ecc07b4213c2b2a558b594f4f486c4ad8e991671e#eventlog
    select 
        block_date
        , sum(varbinary_to_uint256(varbinary_substring(data,1,32))/1e18) as daily_claim
    from ethereum.logs
    where contract_address = 0x4316A00D31da1313617DbB04fD92F9fF8D1aF7Db -- Blast: Lido Yield Provider
        and topic0 = 0x41628d0ba42442e4aa4fc514eeb97bb7154969e70e6678229c836f3b9732ba90 -- Claimed
        and block_date >= date '2024-02-24' -- day when blast L1 bridge is deployed
    group by 1
)

, daily_eth_yield as (
    -- example: https://etherscan.io/tx/0xd55ff8f9eaf4867d126bfec77e5d7d15200d22565259f0b7e013b897ca02e92b#eventlog
    select 
        block_date
        , sum(varbinary_to_int256(varbinary_substring(data,1,32))/1e18) as daily_yield -- not uint256, because yield can be negative
    from ethereum.logs
    where contract_address = 0x98078db053902644191f93988341E31289E1C8FE -- Blast: ETH Yield Manager Proxy
        and topic0 = 0x00de4b58e7863b1e3dce7259a138136239427388d53e4844f369cdee7a81dbf5 -- YieldReport
        and block_date >= date '2024-02-24' -- day when blast L1 bridge is deployed
    group by 1
)

select 
    *
    , case 
        when balance_in_lido=0 then 0 
        else cast(daily_yield as double) * 365.0/ balance_in_lido
    end as yield_apy
from (
    select 
        block_date
        , coalesce(daily_stake,0) as daily_stake
        , -1*coalesce(daily_claim,0) as daily_claim
        , coalesce(daily_yield,0) as daily_yield
        -- lido_balance: ETH Yield Manager's balance in Lido
        , sum(coalesce(daily_stake,0) - coalesce(daily_claim,0)) over (order by block_date) as balance_in_lido
    -- sequence includes both ends
    -- start day is when blast L1 bridge is deployed
    from unnest(sequence(date '2024-02-24', current_date - interval '1' day, interval '1' day)) as days(block_date)
    left join daily_staked_to_lido stake using (block_date)
    left join daily_claim_from_lido claim using (block_date)
    left join daily_eth_yield yield using (block_date)
)
