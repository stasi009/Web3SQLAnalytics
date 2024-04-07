with airdrop_claimed as (
    select 
        block_time
        -- ! NOTE: 不能用varbinary_ltrim去除地址前边的0，因为有的地址就是以0开头的
        , varbinary_substring(data,1+32+12,20) as claimer 
        , varbinary_to_uint256(varbinary_substring(data,1+2*32,32)) / 1e18 as claim_op
    from optimism.logs
    where block_date >= date '2024-02-16' -- day when airdrop contract is deployed
        and contract_address = 0xFb4D5A94b516DF77Fbdbcf3CfeB262baAF7D4dB7 -- airdrop contract
        and topic0 = 0x4ec90e965519d92681267467f775ada5bd214aa92c0dc93d90a5e880ce9ed026 -- claimed
)

, first_claim as (
    select min(block_time) as first_claim_tm
    from airdrop_claimed
)

--- ################################ delegate change
, delegate_change_for_claimers as (
    -- 只关心claimer的delegate change情况(包括airdrop前与后)。注意！可能有些claimer之前与之后都没有delegate过
    select
        del.evt_block_time
        , del.delegator 
        , del.toDelegate 
    from op_optimism.GovernanceToken_evt_DelegateChanged del
    inner join airdrop_claimed ac  
        on del.delegator = ac.claimer 
)

, claimer_delegate_before_airdrop as (
    select 
        delegator
        , toDelegate as delegate_before_airdrop
    from (
        select
            del.delegator 
            , del.toDelegate 
            , row_number() over (partition by del.delegator order by del.evt_block_time desc) as rank
        from delegate_change_for_claimers del
        cross join first_claim fc
        where del.evt_block_time < fc.first_claim_tm
    )
    where rank = 1
)

, claimer_delegate_current as (
    select 
        delegator
        , toDelegate as delegate_after_airdrop
    from (
        select
            del.delegator 
            , del.toDelegate 
            , row_number() over (partition by del.delegator order by del.evt_block_time desc) as rank
        from delegate_change_for_claimers del
    )
    where rank = 1
)

--- ################################ op balance change
, op_transfer as (
    select
        evt_block_time as block_time 
        , "from"
        , to
        , cast(tf.value as double)/1e18 as op_amount
    from erc20_optimism.evt_transfer tf
    where contract_address = 0x4200000000000000000000000000000000000042 -- OP Token
)

, op_flow_with_claimers as (
    select
        tf.block_time 
        , ac.claimer as account
        , tf.op_amount
    from op_transfer tf
    inner join airdrop_claimed ac   
        on tf.to = ac.claimer


    union all 

    select 
        tf.block_time
        , ac.claimer as account
        , -tf.op_amount as op_amount
    from op_transfer tf
    inner join airdrop_claimed ac   
        on tf."from" = ac.claimer
)

, claimer_op_before_airdrop as (
    select 
        account 
        , sum(op_amount) as op_before_airdrop
    from op_flow_with_claimers opf
    cross join first_claim fc 
    where opf.block_time < fc.first_claim_tm
    group by 1
)

, claimer_op_current as (
    select 
        account 
        , sum(op_amount) as op_after_airdrop
    from op_flow_with_claimers opf
    group by 1
)

--- ################################ combine
select 
    ac.claimer
    , ac.claim_op

    , delegate_before_airdrop
    , delegate_after_airdrop

    , op_before_airdrop
    , op_after_airdrop
from airdrop_claimed ac  
left join claimer_delegate_before_airdrop bfd
    on ac.claimer = bfd.delegator
left join claimer_delegate_current afd
    on ac.claimer = afd.delegator
left join claimer_op_before_airdrop bfo 
    on ac.claimer = bfo.account 
left join claimer_op_current afo 
    on ac.claimer = afo.account 
