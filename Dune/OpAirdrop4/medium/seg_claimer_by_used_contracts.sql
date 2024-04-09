
select
    tx.blockchain
    , tx.block_time
    , tx.hash
    , tx."from" as claimer
    , tx.to as contract_address
    , ct.contract_name
    , ct.contract_project
from evms.transactions tx
inner join optimism_airdrop_4_optimism.MerkleDistributor_evt_Claimed as ac 
    on tx."from" = ac.account
inner join contracts.contract_mapping ct
    on tx.to = ct.contract_address
    and tx.blockchain = ct.blockchain
where tx.block_time >= current_date - interval '{{backdays}}' day
    and success