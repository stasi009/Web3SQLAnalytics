
-- 这个query回答这样的问题：
-- 如果我在ethereum上向Arbitrum Delayed Inbox转移eth,想把它们转移到arbitrum上去
-- arbitrum上的eth是从哪里来的？
-- query本身并不能准确回答这一问题，只能缩小范围，再人为排查，答案是：通过
-- 0x80c67432656d59144ceff962e8faf8926599bcf8	Orbiter Finance: Bridge	
-- 0xe4edb277e41dc89ab076a1f049f4a3efa700bce8	Orbiter Finance: Bridge 3
-- 这两个地址发送到arbitrum链上来的

with ethreum_send_txn as (-- 由EOA主动向arbitrum bridge发起的transfer
    select 
        block_time as send_time
        , get_href(get_chain_explorer_tx_hash('ethereum', hash), 'send tx') as send_tx_link
        , "from" as sender
        , value/1e18 as send_amount
    from ethereum.transactions
    where to = 0x4Dbd4fc535Ac27206064B68FfCf827b0A60BAB3f -- Arbitrum Delayed Inbox
        and value > 0
        and block_date between current_date - interval '1' day and current_date
        and success
)

, arbitrum_recv_txn as (
    select 
        block_time as recv_time 
        , get_href(get_chain_explorer_tx_hash('arbitrum', hash), 'recv tx') as recv_tx_link
        , to as receiver
        , value/1e18 as recv_amount
    from arbitrum.transactions
    where "from" in (
        0x80c67432656d59144ceff962e8faf8926599bcf8
        , 0xe4edb277e41dc89ab076a1f049f4a3efa700bce8
    )
        and value > 0 
        and block_date between current_date - interval '1' day and current_date
        and success
        and varbinary_length(data) = 0 -- just transfer, no method call, also because 'to' is EOA, not contract
)

select 
    sd.send_time 
    , sd.send_tx_link
    , rv.recv_time
    , rv.recv_tx_link

    , date_diff('minute',sd.send_time,rv.recv_time) as elapsed_minutes

    , sd.send_amount
    , rv.recv_amount
    , (rv.recv_amount/sd.send_amount-1) as amt_change_percent
from ethreum_send_txn sd
left join arbitrum_recv_txn rv
    on rv.recv_time between sd.send_time and sd.send_time + interval '5' minute -- 最多等5分钟
    and rv.recv_amount between sd.send_amount * 0.8 and sd.send_amount * 1.1
order by 1