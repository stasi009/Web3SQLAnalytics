
with wallet_contracts as (
    select 
        address
    from labels.contracts c
    where blockchain='ethereum'
        and (lower(c.name) LIKE '%argent%' -- argent wallet
            OR lower(c.name) LIKE '%aragon%' -- Aragon: Govern for DAO
            )
)

select 
    cr.address 
from ethereum.creation_traces as cr 
left join safe_ethereum.safes as sf 
    on cr.address = sf.address 
left join wallet_contracts as wa 
    on cr.address = wa.address
where sf.address is null -- cannot match safe contracts
    and wa.address is null -- cannot match wallet contracts