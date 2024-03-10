-- transfer eth时，txn有value字段，而trace也有value字段
-- 要搞清楚，这两个value是什么关系
with traces as (
    select 
        r.block_time as block_time 
        ,get_href(get_chain_explorer_tx_hash('ethereum', r.tx_hash), 'tx link') as tx_link
        ,r.trace_address

        ,t."from" AS tx_from
        ,t.to AS tx_to

        ,r."from" as trace_from
        ,r.to as trace_to


        ,r.value as trace_value
        ,t.value as txn_value

        ,bytearray_substring(t.data, 1, 4) as tx_method_id
    from ethereum.traces r  
    inner join ethereum.transactions t
        on r.tx_hash = t.hash
        and r.block_number = t.block_number
    where
        (r.call_type not in ('delegatecall', 'callcode', 'staticcall') or r.call_type is null)
        and r.tx_success
        and r.success
        and r.value > 0
        and r.block_date = current_date
)

select *  
from traces
where varbinary_length(tx_method_id) = 0 -- just transfer no method call
order by block_time desc
limit 100
