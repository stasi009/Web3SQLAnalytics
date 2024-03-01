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
        'ethereum' as blockchain
        , block_date
        , total_fee_usd / num_txn as avg_txn_fee_usd
    from (
        select 
            block_date
            , count(hash) as num_txn
            , sum(txn.gas_price / 1e18 * txn.gas_used * p.price) as total_fee_usd
        from ethereum.transactions txn
        inner join prices_usd p
            on p.minute = date_trunc('minute',txn.block_time)
        where txn.block_date between current_date - interval '{{back_days}}' day and current_date - interval '1' day
            and p.blockchain is null 
            and p.symbol = 'ETH'
        group by 1
    )
)

, arbitrum_daily_fee as (
    select 
        'arbitrum' as blockchain
        , block_date
        , total_fee_usd / num_txn as avg_txn_fee_usd
    from (
        select 
            block_date
            , count(hash) as num_txn
            , sum(txn.effective_gas_price / 1e18 * txn.gas_used * p.price) as total_fee_usd
        from arbitrum.transactions txn
        inner join prices_usd p
            on p.minute = date_trunc('minute',txn.block_time)
        where txn.block_date between current_date - interval '{{back_days}}' day and current_date - interval '1' day
            and p.blockchain is null 
            and p.symbol = 'ETH'
        group by 1
    )
)

, 

select * from arbitrum_daily_fee
