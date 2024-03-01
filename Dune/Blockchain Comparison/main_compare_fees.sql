with prices_usd as (
    select
        minute 
        , blockchain
        , symbol
        , price 
    from prices.usd
    where 
        symbol in ('ETH','SOL','AVAX','MATIC')
        and date_trunc('day',minute) between current_date - interval '{{back_days}}' day and current_date - interval '1' day
)

, ethereum_daily_fee as (
    select 
        block_date
        , count(hash) as num_txn
        , count(hash) filter (where success) as num_success_txn
        , count(distinct "from") as num_users
    from ethereum.transactions txn
    inner join prices_usd p
        on p.minute = date_trunc('minute',txn.block_time)
    where txn.block_date between current_date - interval '{{back_days}}' day and current_date - interval '1' day
        and p.blockchain is null 
        and p.symbol = 'ETH'
    group by 1
)