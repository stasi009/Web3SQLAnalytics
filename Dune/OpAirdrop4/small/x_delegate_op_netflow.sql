
with airdrop_claimed as (
    select 
        block_time
        -- ! NOTE: 不能用varbinary_ltrim去除地址前边的0，因为有的地址就是以0开头的
        , varbinary_substring(data,1+32+12,20) as claimer 
        , varbinary_to_uint256(varbinary_substring(data,1+2*32,32)) / 1e18 as op_amount
    from optimism.logs
    where block_date >= date '2024-02-16' -- day when airdrop contract is deployed
        and contract_address = 0xFb4D5A94b516DF77Fbdbcf3CfeB262baAF7D4dB7 -- airdrop contract
        and topic0 = 0x4ec90e965519d92681267467f775ada5bd214aa92c0dc93d90a5e880ce9ed026 -- claimed
)

, first_claim as (
    select 
        min(block_time) as first_claim_tm
    from airdrop_claimed
)

, op_inflow as (
    select 
        tf.to as delegate
        , sum(cast(tf.value as double))/1e18 as op_amount
    from erc20_optimism.evt_transfer tf
    inner join op_governance_optimism.delegates_addresses deladdr
        on tf.to = deladdr.address -- 只关心发向delgate的,主要是来自airdrop合约
    cross join first_claim fc
    where tf.evt_block_time >= fc.first_claim_tm
        and contract_address = 0x4200000000000000000000000000000000000042 -- OP token
    group by 1
)

, op_outflow as (
    select 
        tf."from" as delegate
        , sum(cast(tf.value as double))/1e18 as op_amount
    from erc20_optimism.evt_transfer tf
    inner join op_governance_optimism.delegates_addresses deladdr
        on tf."from" = deladdr.address -- 只关心由delegate向外发出的op，这代表了voting power下降的主流情况
    cross join first_claim fc
    where tf.evt_block_time >= fc.first_claim_tm
        and contract_address = 0x4200000000000000000000000000000000000042 -- OP token
    group by 1
)

select 
    get_href(get_chain_explorer_address('optimism', deladdr.address),coalesce(deladdr.ens, deladdr.name)) as delegate
    , coalesce(ac.op_amount,0) as claimed_op
    , coalesce(inf.op_amount, 0) as total_inflow_op
    , -1*coalesce(outf.op_amount, 0) as total_outflow_op
    , coalesce(inf.op_amount, 0)  - coalesce(outf.op_amount, 0) as netflow_op
from op_governance_optimism.delegates_addresses deladdr
left join airdrop_claimed ac
    on deladdr.address = ac.claimer
left join op_inflow inf 
    on deladdr.address = inf.delegate 
left join op_outflow as outf 
    on deladdr.address = outf.delegate
order by total_outflow_op
