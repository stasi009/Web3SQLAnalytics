
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
        , count(proposal_id) over (partition by voter order by date_timestamp) as cumsum_votes
    from deduplicated_vote_data
)



select *  
from voter_stats
where voter = 0x46abfe1c972fca43766d6ad70e1c1df72f4bb4d1
order by current_vote_tm