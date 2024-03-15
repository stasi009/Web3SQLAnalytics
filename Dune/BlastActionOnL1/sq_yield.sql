
select 
    varbinary_to_int256(varbinary_substring(data,1,32))/1e18 as yield -- not uint256, because yield can be negative
    , varbinary_to_uint256(varbinary_substring(data,1+32,32))/1e18 as insurancePremiumPaid
    , varbinary_to_uint256(varbinary_substring(data,1+2*32,32))/1e18 as insuranceWithdrawn
from ethereum.logs
where contract_address in (
        0x98078db053902644191f93988341E31289E1C8FE -- Blast: ETH Yield Manager Proxy
        , 0xa230285d5683C74935aD14c446e137c8c8828438 -- Blast: USD Yield Manager Proxy
        ) 
    and topic0 = 0x00de4b58e7863b1e3dce7259a138136239427388d53e4844f369cdee7a81dbf5 -- YieldReport
    and block_date >= date '2024-02-24' -- day when blast L1 bridge is deployed