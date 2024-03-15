
select 
    varbinary_to_int256(varbinary_substring(data,1,32)) as yield -- not uint256, because yield can be negative
    , varbinary_to_uint256(varbinary_substring(data,1+32,32)) as insurancePremiumPaid
    , varbinary_to_uint256(varbinary_substring(data,1+2*32,32)) as insuranceWithdrawn
from ethereum.logs
where contract_address = 0x98078db053902644191f93988341E31289E1C8FE -- Blast: ETH Yield Manager Proxy
    and topic0 = 0x00de4b58e7863b1e3dce7259a138136239427388d53e4844f369cdee7a81dbf5 -- YieldReport
    and block_date = date '2024-03-11' 
    and tx_hash = 0xd55ff8f9eaf4867d126bfec77e5d7d15200d22565259f0b7e013b897ca02e92b