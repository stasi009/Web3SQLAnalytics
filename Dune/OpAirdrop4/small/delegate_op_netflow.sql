
with airdrop as (
    select 
        min(block_time) as first_claim_tm
    from optimism.logs
    where block_date >= date '2024-02-16' -- day when airdrop contract is deployed
        and contract_address = 0xFb4D5A94b516DF77Fbdbcf3CfeB262baAF7D4dB7 -- airdrop contract
        and topic0 = 0x4ec90e965519d92681267467f775ada5bd214aa92c0dc93d90a5e880ce9ed026 -- claimed
)

, op_inflow as (
    select 
        tf.to as delegate
        , sum(cast(tf.value as double))/1e18 as op_amount
    from erc20_optimism.evt_transfer tf
    inner join op_governance_optimism.delegates_addresses deladdr
        on tf.to = deladdr.address -- 只关心发向delgate的,主要是来自airdrop合约
    cross join airdrop ad
    where tf.evt_block_time >= ad.first_claim_tm
    group by 1
)

, op_outflow as (
    select 
        tf."from" as delegate
        , sum(cast(tf.value as double))/1e18 as op_amount
    from erc20_optimism.evt_transfer tf
    inner join op_governance_optimism.delegates_addresses deladdr
        on tf."from" = deladdr.address -- 只关心由delegate向外发出的op，这代表了voting power下降的主流情况
    cross join airdrop ad
    where tf.evt_block_time >= ad.first_claim_tm
    group by 1
)

select 
    deladdr.address as delegate 
    , coalesce(deladdr.ens, deladdr.name) as delegate_name 

    , coalesce(inf.op_amount, 0) as inflow_op
    , -1*coalesce(outf.op_amount, 0) as outflow_op
    , coalesce(inf.op_amount, 0)  - coalesce(outf.op_amount, 0) as netflow_op
from op_governance_optimism.delegates_addresses deladdr
left join op_inflow inf 
    on deladdr.address = inf.delegate 
left join op_outflow as outf 
    on deladdr.address = outf.delegate
order by outflow_op


