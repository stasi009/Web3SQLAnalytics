with evt_claimed as (
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

, opt_amt_range as (
    select 
        min(op_amt_adjdec) as min_amt
        , (max(op_amt_adjdec) - min(op_amt_adjdec))/{{bins}} as bin_width
    from evt_claimed
)

, op_amt_bins as (
    select 
        idx
        , r.min_amt + (idx-1)*r.bin_width as lb
        , r.min_amt + idx * r.bin_width as ub
    from unnest(sequence(1,{{bins}})) as tbl(idx)
    cross join opt_amt_range r
)

select 
    b.idx as bin_idx
    , round(b.lb,2) as bin_lb
    , count(c.claimer) as num_claimers
from op_amt_bins b
left join evt_claimed c
    on c.op_amt_adjdec >= b.lb 
    and c.op_amt_adjdec < b.ub
group by 1,2
order by 1

