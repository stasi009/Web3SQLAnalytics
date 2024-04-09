
with nft_trades as (
    select 
        blockchain
        , project_contract_address
        , project
        , t.trader
    from nft.trades
    cross join unnest(array[buyer, seller]) as t(trader)
    where blockchain in ('optimism', 'ethereum', 'zora', 'base')
        and block_time >= current_date - interval '{{backdays}}' day
        and block_time < current_date -- avoid incomplete date
)

, claimer_trades as (
    select 
        ac.account as claimer

        , t.blockchain
        , t.project_contract_address
        , t.project

    from optimism_airdrop_4_optimism.MerkleDistributor_evt_Claimed as ac 
    inner join nft_trades t
        on ac.account = t.trader
)

select 
    project
    , count(distinct claimer) as num_claimers
from claimer_trades
group by 1
order by num_claimers desc


