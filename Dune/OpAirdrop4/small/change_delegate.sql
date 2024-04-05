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
        varbinary_ltrim(varbinary_substring(data,1+32,32)) as claimer 
        -- , varbinary_to_uint256(varbinary_substring(data,1+2*32,32)) / 1e18 as op_amt_adjdec
    from optimism.logs
    where block_date >= date '2024-02-16' -- day when airdrop contract is deployed
        and contract_address = 0xFb4D5A94b516DF77Fbdbcf3CfeB262baAF7D4dB7 -- airdrop contract
        and topic0 = 0x4ec90e965519d92681267467f775ada5bd214aa92c0dc93d90a5e880ce9ed026 -- claimed
)

, delegate_change_for_claimers as (
    select
        del.evt_block_time
        , del.delegator 
        , del.toDelegate 
    from op_optimism.GovernanceToken_evt_DelegateChanged del
    inner join airdrop_claimed ac  
        on del.delegator = ac.claimer -- 只关心claimer的delegate change情况(包括airdrop前与后)
)

, claimer_delegate_before_airdrop as (
    select 
        delegator
        , toDelegate as delegate_before_airdrop
    from (
        select
            del.delegator 
            , del.toDelegate 
            , rank() over (partition by del.delegator order by del.evt_block_time desc) as rank
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
            , rank() over (partition by del.delegator order by del.evt_block_time desc) as rank
        from delegate_change_for_claimers del
        cross join start_claim sc
        where del.evt_block_time >= sc.start_claim_tm
    )
    where rank = 1
)

, claimer_delegate_changes as (
    select 
        delegator
        , delegate_before_airdrop
        , delegate_after_airdrop
    from claimer_delegate_before_airdrop
    full outer join claimer_delegate_after_airdrop
        using (delegator)
)

select * from claimer_delegate_changes