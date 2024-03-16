
with L2proxyOnL1 as (
    SELECT
        value[0]::string as address
        , value[1]::string as name
        , value[2]::string as l2chain 
    FROM (
        SELECT
            livequery.live.udf_api('https://flipsidecrypto.xyz/api/queries/bca7fe3b-c929-4144-82e2-dc3378c3fd7a/latest-run') as response
        ), lateral FLATTEN (input => response:data:result:rows)
)

select 
    date_trunc('hour',txns.block_timestamp) as hour 
    , coalesce(l2.l2chain,'L1 or other L2') as l2chain
    
    , count(txns.tx_hash) as num_txn

    , avg(gas_used) as avg_gas_used
    , avg(tx_fee) as avg_tx_fee_eth

    , APPROX_PERCENTILE(gas_used,0.5) as median_gas_used
    , APPROX_PERCENTILE(tx_fee,0.5) as median_tx_fee_eth

    , avg(gas_price) as avg_gas_price_gwei
from ethereum.core.fact_transactions txns
left join L2proxyOnL1 l2
    on txns.to_address = l2.address 
where txns.block_timestamp::date >= '2024-03-13'
group by 1,2
order by 1,2