-- https://dune.com/queries/3528338
select 
    block_date
    , tx_hash

    , case 
        when contract_address = 0x98078db053902644191f93988341E31289E1C8FE then 'ETH'
        when contract_address = 0xa230285d5683C74935aD14c446e137c8c8828438 then 'USD'
    end as yield_currency

    , case 
        when contract_address = 0x98078db053902644191f93988341E31289E1C8FE then 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2 -- use WETH to query price
        when contract_address = 0xa230285d5683C74935aD14c446e137c8c8828438 then 0x6B175474E89094C44Da98b954EedeAC495271d0F -- use Dai to query price
    end as price_token

    , varbinary_to_int256(varbinary_substring(data,1,32))/1e18 as yield -- not uint256, because yield can be negative
    , varbinary_to_uint256(varbinary_substring(data,1+32,32))/1e18 as insurancePremiumPaid
    , varbinary_to_uint256(varbinary_substring(data,1+2*32,32))/1e18 as insuranceWithdrawn
from ethereum.logs
where contract_address in (
        -- example: https://etherscan.io/tx/0xd55ff8f9eaf4867d126bfec77e5d7d15200d22565259f0b7e013b897ca02e92b#eventlog
        0x98078db053902644191f93988341E31289E1C8FE -- Blast: ETH Yield Manager Proxy
        -- example: https://etherscan.io/tx/0x55766ee1cf72625691d694fdc32758efe75a2f1e1959e6d3c88d8554d794056f#eventlog
        , 0xa230285d5683C74935aD14c446e137c8c8828438 -- Blast: USD Yield Manager Proxy
        ) 
    and topic0 = 0x00de4b58e7863b1e3dce7259a138136239427388d53e4844f369cdee7a81dbf5 -- YieldReport
    and block_date >= date '2024-02-24' -- day when blast L1 bridge is deployed