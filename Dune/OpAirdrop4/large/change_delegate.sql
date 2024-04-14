
with delegate_changes as (
    select
        dc.evt_tx_hash as tx_hash
        , dc.evt_block_time as block_time

        , dc.delegator 
        , toD.power_diff as delegator_op_balance

        , dc.fromDelegate 
        , fromD.previous_voting_power as fromDelVotePower
        
        , dc.toDelegate
        , toD.previous_voting_power as toDelVotePower

    from op_optimism.GovernanceToken_evt_DelegateChanged dc
    inner join op_governance_optimism.delegates fromD 
        on dc.evt_tx_hash = fromD.tx_hash 
        and dc.fromDelegate = fromD.delegate 
    inner join op_governance_optimism.delegates toD
        on dc.evt_tx_hash = toD.tx_hash 
        and dc.toDelegate = toD.delegate 
)
