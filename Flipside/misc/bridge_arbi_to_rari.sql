
-- Rari chain mainnet在2024-01-24上线
-- 但是无论如何查询，通过已知bridge，都无法查询到向rari transfer eth的记录

select *   
from arbitrum.defi.ez_bridge_activity
where DESTINATION_CHAIN_ID = 1380012617
    and block_timestamp >= date '2024-01-24'

select *   
from arbitrum.defi.ez_bridge_activity
where lower(DESTINATION_CHAIN) like '%rari%'
    and block_timestamp >= date '2024-01-24'

select 
    DESTINATION_CHAIN
    , count(tx_hash) as counter
from arbitrum.defi.ez_bridge_activity
where block_timestamp::date >= date '2024-01-24' 
group by 1 
order by 2 desc