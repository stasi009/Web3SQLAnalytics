with vote_power_decrease as (
    select 
        block_time
        , tx_hash
        , delegate
        , power_diff
    from op_governance_optimism.delegates
    where power_diff < 0
)
, op_transfer as (
    select 
        evt_tx_hash
        , tf."from"
        , cast(tf.value as double)/1e18 as op_amount
    from erc20_optimism.evt_transfer tf
    left join op_governance_optimism.delegates_addresses deladdr
        on tf."from" = deladdr.address
    where deladdr.address is null  -- 不是由delegate向外发出
        and tf."from" <> 0x0000000000000000000000000000000000000000
)

select
    vpd.block_time
    , vpd.tx_hash
    , tf."from" as delegator
    , vpd.delegate
    , vpd.power_diff
    , tf.op_amount as transfer_op
from vote_power_decrease vpd 
inner join op_transfer tf 
    on vpd.tx_hash = tf.evt_tx_hash
where vpd.block_time >= date '2024-04-01'
order by transfer_op desc 
limit 100