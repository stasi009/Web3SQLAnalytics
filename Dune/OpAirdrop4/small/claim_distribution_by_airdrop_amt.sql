-- https://dune.com/queries/3594147/
-- 根据要airdrop amount（要airdrop而非实际claim）划分区间统计各区间的claim人数与数量

with opt_amt_range as (
    -- 这里将lower bound比actual minimum更小，upper bound比actual maximum更大
    -- 这样保证[lb, ub)能够囊括全部范围
    select 
        min(total_op)*0.99 as min_amt
        , (max(total_op)*1.01 - min(total_op)*0.99)/{{bins}} as bin_width
    from dune.oplabspbc.dataset_op_airdrop_4_simple_list -- all addresses qualified for airdrop4
)

, op_amt_bins as (
    select 
        bin_idx
        , r.min_amt + (bin_idx-1)*r.bin_width as lb
        , r.min_amt + bin_idx * r.bin_width as ub
    from unnest(sequence(1,{{bins}})) as tbl(bin_idx)
    cross join opt_amt_range r
)

, evt_claimed as (
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

, all_qualified_accounts_extend as (
    select  
        all.address 
        , all.total_op

        , if(cl.claimer is null,0,1) as is_claimed
        , coalesce(cl.op_amt_adjdec,0) as claimed_op

        , bins.bin_idx
        , bins.lb as bin_lb
        , bins.ub as bin_ub
    from dune.oplabspbc.dataset_op_airdrop_4_simple_list all -- all addresses qualified for airdrop4
    left join evt_claimed cl
        on all.address = cl.claimer
    left join op_amt_bins bins
        on all.total_op >= bins.lb 
        and all.total_op < bins.ub
)

select 
    bin_idx
    , '[' || cast(cast(bin_lb as int) as varchar) || ',' || cast(cast(bin_ub as int) as varchar) || ')' as bin_label

    , count(address) as total_qualified_accounts
    , sum(is_claimed) as num_claimed_accounts
    , count(address) - sum(is_claimed) as not_claimed_accounts
    , cast(sum(is_claimed) as double)/count(address) as claimed_user_ratio

    , sum(total_op) as total_op 
    , sum(claimed_op) as claimed_op
    , sum(total_op) - sum(claimed_op) as not_claimed_op
    , sum(claimed_op) / sum(total_op) as claimed_op_ratio
from all_qualified_accounts_extend 
group by 1,2
order by 1

