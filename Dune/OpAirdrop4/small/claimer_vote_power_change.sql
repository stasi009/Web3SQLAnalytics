-- https://dune.com/queries/3598102
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

--- ################################ delegate change
, delegate_change_for_claimers as (
    -- 只关心claimer的delegate change情况(包括airdrop前与后)。注意！可能有些claimer之前与之后都没有delegate过
    select
        del.evt_block_time as delegate_tm
        , del.delegator 
        , del.toDelegate 
        , ac.block_time as claim_tm
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
            , row_number() over (partition by del.delegator order by del.delegate_tm desc) as rank
        from delegate_change_for_claimers del
        where delegate_tm < claim_tm -- delegate change before claim airdrop
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
            , row_number() over (partition by del.delegator order by del.delegate_tm desc) as rank
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
        tf.block_time as transfer_tm
        , ac.claimer as account
        , tf.op_amount
        , ac.block_time as claim_tm
    from op_transfer tf
    inner join airdrop_claimed ac   
        on tf.to = ac.claimer


    union all 

    select 
        tf.block_time as transfer_tm
        , ac.claimer as account
        , -tf.op_amount as op_amount
        , ac.block_time as claim_tm
    from op_transfer tf
    inner join airdrop_claimed ac   
        on tf."from" = ac.claimer
)

, claimer_op_before_airdrop as (
    select 
        account 
        , sum(op_amount) as op_before_airdrop
    from op_flow_with_claimers opf
    where transfer_tm < claim_tm -- transfer before claim airdrop
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
, claimer_vote_power_changes as (
    select 
        *
        , is_delegated_pre_ad * op_before_airdrop as vote_power_pre_ad
        , is_delegated_post_ad * op_after_airdrop as vote_power_post_ad
    from (
        select 
            ac.claimer
            , ac.claim_op

            , delegate_before_airdrop
            , if(delegate_before_airdrop is null or delegate_before_airdrop = 0x0000000000000000000000000000000000000000,0,1) as is_delegated_pre_ad
            , delegate_after_airdrop
            , if(delegate_after_airdrop is null or delegate_after_airdrop = 0x0000000000000000000000000000000000000000,0,1) as is_delegated_post_ad

            , coalesce(op_before_airdrop,0) as op_before_airdrop
            , coalesce(op_after_airdrop,0) as op_after_airdrop
        from airdrop_claimed ac  
        left join claimer_delegate_before_airdrop bfd
            on ac.claimer = bfd.delegator
        left join claimer_delegate_current afd
            on ac.claimer = afd.delegator
        left join claimer_op_before_airdrop bfo 
            on ac.claimer = bfo.account 
        left join claimer_op_current afo 
            on ac.claimer = afo.account 
    )
)

select 
    *  
    -- !这里没有使用abs，尽管由于浮点数计算误差，有的account balance是-6e-16这个级别的负数
    -- 但是考虑到这种极小的负数也能够被<1e-6处理，所以这里也就没使用abs
    , case 
        when vote_power_pre_ad <= 1e-6 and vote_power_post_ad > 1e-6
            then 'Begin Delegate'
        when vote_power_pre_ad > 1e-6 and vote_power_post_ad <= 1e-6
            then 'Quit Delegate'
        when vote_power_pre_ad <= 1e-6 and vote_power_post_ad <= 1e-6
            then 'Still Not Delegate'
        when delegate_before_airdrop = delegate_after_airdrop 
            then 'Keep Same Delegate'
        when delegate_before_airdrop <> delegate_after_airdrop 
            then 'Change Delegate'  
    end as change_summary
from claimer_vote_power_changes