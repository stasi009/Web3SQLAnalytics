
with sell_op_token as (
    select 
        block_time 
        , block_date

        , taker as seller
        
        , project_contract_address
        , project

        , amount_usd
        , token_sold_amount -- has already adjusted decimals

        , token_bought_address 
        , token_bought_symbol
    from dex.trades
    where blockchain = 'optimism'
        and token_sold_address = 0x4200000000000000000000000000000000000042 -- OP Token
        and block_date >= date '2024-01-01' -- limit time range to reduce data size
)

, claim_airdrop as (
    select
        evt_block_time 
        , account as claimer
        , cast(amount as double)/1e18 as claim_op
    from optimism_airdrop_4_optimism.MerkleDistributor_evt_Claimed 
)

, claimer_sell_op as (
    select 
        so.seller
        , so.token_sold_amount as sold_op -- already adjusted decimals
        , so.amount_usd as trade_usd

        , so.token_bought_address
        , so.token_bought_symbol
    from claim_airdrop ca
    inner join sell_op_token so
        on so.seller = ca.claimer 
        and so.block_time >= ca.evt_block_time -- sell after claim op
)

, claim_op_summary as (
    select 
        count(ca.claimer) as total_claimers
        , sum(ca.claim_op) as total_claim_op
    from claim_airdrop ca
)

, sold_op_summary as (
    select 
        count(distinct seller) as total_sellers
        , sum(sold_op) as total_sold_op   
    from claimer_sell_op
)


select 
    cs.*   
    , ss.*
from claim_op_summary cs 
cross join sold_op_summary ss


