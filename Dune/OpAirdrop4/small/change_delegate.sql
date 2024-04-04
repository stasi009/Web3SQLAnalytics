with start_claim as (
    select 
        min(block_time) as start_claim_tm
    from optimism.logs
    where block_date >= date '2024-02-16' -- day when airdrop contract is deployed
        and contract_address = 0xFb4D5A94b516DF77Fbdbcf3CfeB262baAF7D4dB7 -- airdrop contract
        and topic0 = 0x4ec90e965519d92681267467f775ada5bd214aa92c0dc93d90a5e880ce9ed026 -- claimed
)

, Delegate_Before_Airdrop as (
    select 
        delegator
        , toDelegate as delegate_before_airdrop
    from (
        select
            del.delegator 
            , del.toDelegate 
            , rank() over (partition by del.delegator order by del.evt_block_time desc) as rank
        from op_optimism.GovernanceToken_evt_DelegateChanged del
        cross join start_claim sc
        where del.evt_block_time < sc.start_claim_tm
    )
    where rank = 1
)

, Delegate_After_Airdrop as (
    select 
        delegator
        , toDelegate as delegate_after_airdrop
    from (
        select
            del.delegator 
            , del.toDelegate 
            , rank() over (partition by del.delegator order by del.evt_block_time desc) as rank
        from op_optimism.GovernanceToken_evt_DelegateChanged del
        cross join start_claim sc
        where del.evt_block_time >= sc.start_claim_tm
    )
    where rank = 1
)

, Delegate_BeforeAfter_Airdrop as (
    select 
        delegator
        , delegate_before_airdrop
        , delegate_after_airdrop
    from Delegate_Before_Airdrop
    full outer join Delegate_After_Airdrop
        using (delegator)
)

select * from Delegate_BeforeAfter_Airdrop




