
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
    where dc.fromDelegate <> dc.toDelegate
)

, deduplicated_vote_data as (--governance_optimism.proposal_votes有重复的脏数据
    select 
        tx_hash
        , date_timestamp
        , voter 
        , proposal_id
        -- , arbitrary(votingweightage) as votingweightage -- 重复的，取任意值都可以
        -- , arbitrary(choice_name) as choice_name
    from governance_optimism.proposal_votes
    group by 1,2,3,4
)

, voter_stats as (--每次投票时该voter的统计状况
    select
        date_timestamp as current_vote_tm
        , lead(date_timestamp,1,now()) over (partition by voter order by date_timestamp) as next_vote_tm
        , voter 
        , proposal_id
        , count(proposal_id) over (partition by voter order by date_timestamp) as cumsum_voted
    from deduplicated_vote_data
)

, delegate_changes_votenum as (
    select 
        dc.tx_hash
        , dc.block_time

        , dc.delegator 
        , dc.delegator_op_balance

        , dc.fromDelegate
        , dc.fromDelVoteWeight
        , coalesce(fv.cumsum_voted,0) as fromDelNumVoted

        , dc.toDelegate
        , dc.toDelVoteWeight
        , coalesce(tv.cumsum_voted,0) as toDelNumVoted
    from delegate_changes dc
    left join voter_stats fv
        on dc.fromDelegate = fv.voter 
        and dc.block_time >= fv.current_vote_tm
        and dc.block_time < fv.next_vote_tm
    left join voter_stats tv 
        on dc.toDelegate = tv.voter
        and dc.block_time >= tv.current_vote_tm
        and dc.block_time < tv.next_vote_tm
)