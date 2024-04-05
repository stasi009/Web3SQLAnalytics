with start_claim as (
    select 
        min(block_time) as start_claim_tm
    from optimism.logs
    where block_date >= date '2024-02-16' -- day when airdrop contract is deployed
        and contract_address = 0xFb4D5A94b516DF77Fbdbcf3CfeB262baAF7D4dB7 -- airdrop contract
        and topic0 = 0x4ec90e965519d92681267467f775ada5bd214aa92c0dc93d90a5e880ce9ed026 -- claimed
)

, airdrop_claimed as (
    select 
        -- tx_hash 
        -- , block_time
        -- ! NOTE: 不能用varbinary_ltrim去除地址前边的0，因为有的地址就是以0开头的
        varbinary_substring(data,1+32+12,20) as claimer 
        -- , varbinary_to_uint256(varbinary_substring(data,1+2*32,32)) / 1e18 as op_amt_adjdec
    from optimism.logs
    where block_date >= date '2024-02-16' -- day when airdrop contract is deployed
        and contract_address = 0xFb4D5A94b516DF77Fbdbcf3CfeB262baAF7D4dB7 -- airdrop contract
        and topic0 = 0x4ec90e965519d92681267467f775ada5bd214aa92c0dc93d90a5e880ce9ed026 -- claimed
)

, delegate_change_for_claimers as (
    -- 只关心claimer的delegate change情况(包括airdrop前与后)。注意！有些claimer之前与之前都没有delegate过
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
        cross join start_claim sc
        where del.evt_block_time < sc.start_claim_tm
    )
    where rank = 1
)

, claimer_delegate_after_airdrop as (
    select 
        delegator
        , toDelegate as delegate_after_airdrop
    from (
        select
            del.delegator 
            , del.toDelegate 
            , row_number() over (partition by del.delegator order by del.evt_block_time desc) as rank
        from delegate_change_for_claimers del
        cross join start_claim sc
        where del.evt_block_time >= sc.start_claim_tm
    )
    where rank = 1
)

, claimer_delegate_changes as (
    select 
        *  
        , case 
            when delegate_before_airdrop = 0x0000000000000000000000000000000000000000 
                and delegate_after_airdrop <> 0x0000000000000000000000000000000000000000
                then 'Begin Delegate'
            when delegate_before_airdrop <> 0x0000000000000000000000000000000000000000 
                and delegate_after_airdrop = 0x0000000000000000000000000000000000000000
                then 'Quit Delegate'
            when delegate_before_airdrop = 0x0000000000000000000000000000000000000000 
                and delegate_after_airdrop = 0x0000000000000000000000000000000000000000
                then 'Still Not Delegate'
            when delegate_before_airdrop = delegate_after_airdrop 
                then 'Keep Same Delegate'
            when delegate_before_airdrop <> delegate_after_airdrop 
                then 'Change Delegate'  
        end as change_after_airdrop
    from (
        select 
            ac.claimer as delegator
            , coalesce(delegate_before_airdrop,0x0000000000000000000000000000000000000000) as delegate_before_airdrop
            , coalesce(delegate_after_airdrop,0x0000000000000000000000000000000000000000) as delegate_after_airdrop
        from airdrop_claimed ac  
        left join claimer_delegate_before_airdrop pread
            on ac.claimer = pread.delegator
        left join claimer_delegate_after_airdrop postad
            on ac.claimer = postad.delegator
    )
    
)

select 
    change_after_airdrop
    , count(delegator) as num_delegators
    , cast(count(delegator) as double) / (sum(count(delegator)) over () ) as delegator_percentage 
from claimer_delegate_changes
group by change_after_airdrop