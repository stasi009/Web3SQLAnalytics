
with claimer_and_his_nfts as (
    select 
        ac.account
        , cast(ac.amount as double)/1e18 as claim_op

        , nft.blockchain
        , nft.creator_label
        , nft.nft_contract_address
        , nft.collection_name
    from optimism_airdrop_4_optimism.MerkleDistributor_evt_Claimed as ac 
    left join query_3452893 nft -- https://dune.com/queries/3452893
        on ac.account = nft.contract_creator_address
)

select 
    blockchain
    , count(distinct account) as num_claimers
from claimer_and_his_nfts
    group by 1
order by num_claimers desc

