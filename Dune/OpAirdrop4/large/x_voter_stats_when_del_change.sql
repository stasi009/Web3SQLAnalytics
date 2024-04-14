
-- 当发生delegate change时，原来的delegate与change to delegate的投票状态
with deduplicated_vote_data as (--governance_optimism.proposal_votes有重复的脏数据
    select 
        tx_hash
        , date_timestamp
        , voter 
        , proposal_id
        , arbitrary(votingweightage) as votingweightage -- 重复的，取任意值都可以
        , arbitrary(choice_name) as choice_name
    from governance_optimism.proposal_votes
    group by 1,2,3,4
)

, voter_stats as (--每次投票时该voter的统计状况
    select
        date_timestamp as current_vote_tm
        , lead(date_timestamp,1,now()) over (partition by voter order by date_timestamp) as next_vote_tm
        , voter 
        , proposal_id
        , choice_name
        , votingweightage
        , count(proposal_id) over (partition by voter order by date_timestamp) as cumsum_voted
    from deduplicated_vote_data
)

, delegate_changes as (
    select 
        * 

        , case 
            when toDelVoteWeight > fromDelVoteWeight then 'W+'
            when toDelVoteWeight < fromDelVoteWeight then 'W-'
            else 'W='
        end as voteweight_chg_mode

        , case 
            when toDelNumVoted > fromDelNumVoted then 'V+'
            when toDelNumVoted < fromDelNumVoted then 'V-'
            else 'V='
        end as numvoted_chg_mode
    
    from (
        select 
            evt_tx_hash
            , dc.delegator 

            , fromDelegate
            , coalesce(fv.votingweightage,0.0) as fromDelVoteWeight
            , coalesce(fv.cumsum_voted,0) as fromDelNumVoted

            , toDelegate
            , coalesce(tv.votingweightage,0.0) as toDelVoteWeight
            , coalesce(tv.cumsum_voted,0) as toDelNumVoted
        from op_optimism.GovernanceToken_evt_DelegateChanged dc
        left join voter_stats fv
            on dc.fromDelegate = fv.voter 
            and dc.evt_block_time >= fv.current_vote_tm
            and dc.evt_block_time < fv.next_vote_tm
        left join voter_stats tv 
            on dc.toDelegate = tv.voter
            and dc.evt_block_time >= tv.current_vote_tm
            and dc.evt_block_time < tv.next_vote_tm
        where dc.fromDelegate <> dc.toDelegate
    )
)

select 
    voteweight_chg_mode
    , numvoted_chg_mode
    , count(evt_tx_hash) as num_changes
from delegate_changes
group by 1,2
order by num_changes desc
