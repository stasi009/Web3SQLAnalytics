
with ethreum_send_txn as (-- 由EOA主动向arbitrum bridge发起的transfer
    select 
        block_time as send_time
        , get_href(get_chain_explorer_tx_hash('ethereum', hash), 'send tx') as send_tx_link
        , "from" as sender
        , value as send_amount
    from ethereum.transactions
    where to = 0x4Dbd4fc535Ac27206064B68FfCf827b0A60BAB3f -- Arbitrum Delayed Inbox
        and value > 0
        and block_date between current_date - interval '1' day and current_date
        and success
)

, arbitrum_recv_traces as (
    select  
        r.block_time as recv_time
        , get_href(get_chain_explorer_tx_hash('arbitrum', r.tx_hash), 'recv tx') as recv_tx_link

        , t."from" as recv_from
        , t.to as recv_to

        , r.value as recv_amount
    from arbitrum.traces r
    inner join arbitrum.transactions t
        on r.tx_hash = t.hash
        and r.block_number = t.block_number
    left join arbitrum.creation_traces cr
        on t.to = cr.address
    where 
        r.block_date between current_date - interval '1' day and current_date
        and t.block_date between current_date - interval '1' day and current_date

        and (r.call_type not in ('delegatecall', 'callcode', 'staticcall') or r.call_type is null)
        and r.value > 0
        and r.tx_success

        and t.success

        and cr.address is null -- txn to EOA, not contract
)

, possible_send_recv_pair as (
    select 
        est.*
        , ar.*
    from ethreum_send_txn est 
    inner join arbitrum_recv_traces ar  
        on ar.recv_time between est.send_time and est.send_time + interval '2' minute 
        and ar.recv_amount > est.send_amount * 0.8 -- 设想最多交20%的手续费 
        and ar.recv_amount < est.send_amount --不用between，因为不想包含两个bound
    order by est.send_time
)

-- select *  
-- from possible_send_recv_pair

-- 通过检查recv_from，发现ethereum transfer out的ETH是通过
-- 0x80c67432656d59144ceff962e8faf8926599bcf8	Orbiter Finance: Bridge	
-- 0xe4edb277e41dc89ab076a1f049f4a3efa700bce8	Orbiter Finance: Bridge 3
-- 这两个地址发送到arbitrum链上来的
select 
    recv_from
    , get_href(get_chain_explorer_address('arbitrum', recv_from), 'recv from') as link 
    , count(recv_from) as counter
from possible_send_recv_pair
group by recv_from 
order by counter desc
