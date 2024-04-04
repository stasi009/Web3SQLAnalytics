
with evt_claimed as (
    select 
        tx_hash 
        , block_time
        , varbinary_ltrim(varbinary_substring(data,1+32,32)) as claimer 
        , varbinary_to_uint256(varbinary_substring(data,1+2*32,32)) as amount
    from optimism.logs
    where block_date >= date '2024-02-16' -- day when airdrop contract is deployed
        and contract_address = 0xFb4D5A94b516DF77Fbdbcf3CfeB262baAF7D4dB7 -- airdrop contract
        and topic0 = 0x4ec90e965519d92681267467f775ada5bd214aa92c0dc93d90a5e880ce9ed026 -- claimed
)

select *   
from evt_claimed
where tx_hash = 0xd54b33a180d9cb2d94aa5e249f418027c0f22c054c87585a4de586886e75f6fb