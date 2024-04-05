
with evt_claimed as (
    select 
        tx_hash 
        , block_time
        -- ! NOTE: 不能用varbinary_ltrim去除地址前边的0，因为有的地址就是以0开头的
        , varbinary_substring(data,1+32+12,20) as claimer 
        , varbinary_to_uint256(varbinary_substring(data,1+2*32,32)) / 1e18 as op_amt_adjdec
    from optimism.logs
    where block_date >= date '2024-02-16' -- day when airdrop contract is deployed
        and contract_address = 0xFb4D5A94b516DF77Fbdbcf3CfeB262baAF7D4dB7 -- airdrop contract
        and topic0 = 0x4ec90e965519d92681267467f775ada5bd214aa92c0dc93d90a5e880ce9ed026 -- claimed
)

, all_qualified_accounts_with_claim_info as (
    select  
        all.address 
        , all.total_op
        , if(cl.claimer is null,0,1) as is_claimed
        , coalesce(cl.op_amt_adjdec,0) as claimed_op
    from dune.oplabspbc.dataset_op_airdrop_4_simple_list all -- all addresses qualified for airdrop4
    left join evt_claimed cl
        on all.address = cl.claimer
)

select 
    count(address) as total_qualified_accounts
    , sum(is_claimed) as num_claimed_accounts
    , cast(sum(is_claimed) as double) * 100 / count(address) as claimed_user_percent

    , sum(total_op) as total_op 
    , sum(claimed_op) as claimed_op
    , sum(claimed_op) * 100 / sum(total_op) as claimed_op_percent
from all_qualified_accounts_with_claim_info