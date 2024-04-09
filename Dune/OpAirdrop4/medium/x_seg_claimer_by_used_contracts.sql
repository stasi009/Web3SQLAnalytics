
with transactions as (
    select *  
    from evms.transactions
    where block_time >= current_date - interval '{{backdays}}' day
        and blockchain in ('optimism', 'ethereum', 'zora', 'base')
        and to not in ( -- 凡是claimer都会与airdrop contract和GovernanceToken交互，不新鲜，排除
            0xfb4d5a94b516df77fbdbcf3cfeb262baaf7d4db7 -- MerkleDistributor, airdrop contract
            , 0x4200000000000000000000000000000000000042 -- GovernanceToken
        )
        and success
)
, contracts as (
    select *  
    from contracts.contract_mapping
    where blockchain in ('optimism', 'ethereum', 'zora', 'base')
)

, claimer_call_contracts as (
    select
        tx.blockchain
        , tx.block_time
        , tx."from" as claimer
        , tx.to as contract_address
        , ct.contract_name
        , ct.contract_project
    from transactions tx
    inner join optimism_airdrop_4_optimism.MerkleDistributor_evt_Claimed as ac 
        on tx."from" = ac.account
    inner join contracts ct
        on tx.to = ct.contract_address
        and tx.blockchain = ct.blockchain
)

select
    blockchain
    , contract_address
    , contract_name
    , contract_project
    , count(claimer) as num_calls
    , count(distinct claimer) as num_claimers
from claimer_call_contracts
group by 1,2,3,4
having count(claimer) > 10
    and count(distinct claimer) > 10
order by num_claimers desc