
with airdrop as (
    select 
        min(block_time) as first_claim_tm
    from optimism.logs
    where block_date >= date '2024-02-16' -- day when airdrop contract is deployed
        and contract_address = 0xFb4D5A94b516DF77Fbdbcf3CfeB262baAF7D4dB7 -- airdrop contract
        and topic0 = 0x4ec90e965519d92681267467f775ada5bd214aa92c0dc93d90a5e880ce9ed026 -- claimed
)

, op_outflow as (
    select 
        tf."from" as delegate
        , coalesce(deladdr.ens,deladdr.name) as delegate_name
        , tf.to as op_receiver
        , cast(tf.value as double)/1e18 as op_amount
    from erc20_optimism.evt_transfer tf
    inner join op_governance_optimism.delegates_addresses deladdr
        on tf."from" = deladdr.address -- 只关心由delegate向外发出的op，这代表了voting power下降的主流情况
    cross join airdrop ad
    where tf.evt_block_time >= ad.first_claim_tm
        and tf.to not in (select address from op_governance_optimism.delegates_addresses)--排除delegate相互转账
)

select 
    delegate 
    , delegate_name 
    , sum(op_amount) as outflow_op_amount
from op_outflow
group by 1,2
order by outflow_op_amount desc
