
select 
    ac.account
    , cast(ac.amount as double)/1e18 as claim_op

    , nft.blockchain
    , nft.creator_label
    , nft.nft_contract_address
    , nft.collection_name
    , nft.token_standard
from optimism_airdrop_4_optimism.MerkleDistributor_evt_Claimed as ac 
left join query_3452893 nft -- https://dune.com/queries/3452893
    on ac.account = nft.contract_creator_address

