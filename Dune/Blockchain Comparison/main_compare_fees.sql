with prices_usd as (
    select
        minute 
        , blockchain
        , symbol
        , price 
    from prices.usd
    where 
        blockchain is null -- for native token
        and symbol in ('ETH','SOL','AVAX','MATIC')
        and date_trunc('day',minute) between current_date - interval '{{back_days}}' day and current_date - interval '1' day
)

, ethereum_daily_fee as (
    select 
        block_date
        , 'ethereum' as blockchain
        , avg(fee_usd) as avg_txnfee_usd
        , approx_percentile(fee_usd, 0.5) as median_txnfee_usd
    from (
        select 
            block_date
            , txn.gas_price / 1e18 * txn.gas_used as fee_native
            , txn.gas_price / 1e18 * txn.gas_used * p.price as fee_usd
        from ethereum.transactions txn
        inner join prices_usd p
            on p.minute = date_trunc('minute',txn.block_time)
        where txn.block_date between current_date - interval '{{back_days}}' day and current_date - interval '1' day
            and p.symbol = 'ETH'
    )
    group by 1
)

, arbitrum_daily_fee as (
    select 
        block_date
        , 'arbitrum' as blockchain
        , avg(fee_usd) as avg_txnfee_usd
        , approx_percentile(fee_usd, 0.5) as median_txnfee_usd
    from (
        select 
            block_date
            , txn.effective_gas_price / 1e18 * txn.gas_used as fee_native
            , txn.effective_gas_price / 1e18 * txn.gas_used * p.price as fee_usd
        from arbitrum.transactions txn
        inner join prices_usd p
            on p.minute = date_trunc('minute',txn.block_time)
        where txn.block_date between current_date - interval '{{back_days}}' day and current_date - interval '1' day
            and p.symbol = 'ETH'
    )
    group by 1
)

, avalanche_c_daily_fee as (
    select 
        block_date
        , 'avalanche_c' as blockchain
        , avg(fee_usd) as avg_txnfee_usd
        , approx_percentile(fee_usd, 0.5) as median_txnfee_usd
    from (
        select 
            block_date
            , txn.gas_price / 1e18 * txn.gas_used as fee_native
            , txn.gas_price / 1e18 * txn.gas_used * p.price as fee_usd
        from avalanche_c.transactions txn
        inner join prices_usd p
            on p.minute = date_trunc('minute',txn.block_time)
        where txn.block_date between current_date - interval '{{back_days}}' day and current_date - interval '1' day
            and p.symbol = 'AVAX'
    )
    group by 1
)

, optimism_daily_fee as (
    select 
        block_date
        , 'optimism' as blockchain
        , avg(fee_usd) as avg_txnfee_usd
        , approx_percentile(fee_usd, 0.5) as median_txnfee_usd
    from (
        select 
            block_date
            , (txn.l1_fee + txn.gas_used * txn.gas_price) / 1e18 as fee_native
            , (txn.l1_fee + txn.gas_used * txn.gas_price) / 1e18 * p.price as fee_usd
        from optimism.transactions txn
        inner join prices_usd p
            on p.minute = date_trunc('minute',txn.block_time)
        where txn.block_date between current_date - interval '{{back_days}}' day and current_date - interval '1' day
            and p.symbol = 'ETH'
    )
    group by 1
)

, polygon_daily_fee as (
    select 
        block_date
        , 'polygon' as blockchain
        , avg(fee_usd) as avg_txnfee_usd
        , approx_percentile(fee_usd, 0.5) as median_txnfee_usd
    from (
        select 
            block_date
            , txn.gas_price / 1e18 * txn.gas_used as fee_native
            , txn.gas_price / 1e18 * txn.gas_used * p.price as fee_usd
        from polygon.transactions txn
        inner join prices_usd p
            on p.minute = date_trunc('minute',txn.block_time)
        where txn.block_date between current_date - interval '{{back_days}}' day and current_date - interval '1' day
            and p.symbol = 'MATIC'
    )
    group by 1
)

select * from polygon_daily_fee

