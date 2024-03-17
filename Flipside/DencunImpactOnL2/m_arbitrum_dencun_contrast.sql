with arbitrum_L1_fee as (
    select 
        date_trunc('hour',txns.block_timestamp) as hour 
        
        , count(txns.tx_hash) as num_txn
        , sum(gas_used) as hourly_gas_used
        , sum(tx_fee) as hourly_tx_fee

    from ethereum.core.fact_transactions txns
    where txns.to_address = lower('0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6') -- 'Arbitrum: Sequencer Inbox'
        and txns.block_timestamp >= '2024-03-10'
        and txns.block_timestamp < date_trunc('hour',current_timestamp) -- reduce impact of incomplete hour
    group by 1
)

, arbitrum_L2_fee as (
    select 
        date_trunc('hour',txns.block_timestamp) as hour 
        
        , count(txns.tx_hash) as num_txn
        , sum(gas_used) as hourly_gas_used
        , sum(tx_fee) as hourly_tx_fee

    from arbitrum.core.fact_transactions txns
    where txns.block_timestamp >= '2024-03-10'
        and txns.block_timestamp < date_trunc('hour',current_timestamp) -- reduce impact of incomplete hour
    group by 1
)

, dex_trades as (
    select 
        date_trunc('hour',block_timestamp) as hour 
        , count(tx_hash) as hourly_trades
        -- , sum(amount_in_usd + amount_out_usd)/2 as trade_volume
    from arbitrum.defi.ez_dex_swaps
    where block_timestamp >= '2024-03-10'
        and block_timestamp < date_trunc('hour',current_timestamp) -- reduce impact of incomplete hour
    group by 1
)

, lending_trades as (
    select 
        date_trunc('hour',block_timestamp) as hour 
        , count(tx_hash) as hourly_trades
        -- , sum(amount_usd) as trade_volume
    from arbitrum.defi.ez_lending_borrows
    where block_timestamp >= '2024-03-10'
        and block_timestamp < date_trunc('hour',current_timestamp) -- reduce impact of incomplete hour
    group by 1
)

, nft_trades as (
    select 
        date_trunc('hour',block_timestamp) as hour 
        , count(tx_hash) as hourly_trades -- 一次tx中可能有多笔交易，所以这里也不必去重
        -- , sum(price_usd) as trade_volume
    from arbitrum.nft.ez_nft_sales
    where block_timestamp >= '2024-03-10'
        and block_timestamp < date_trunc('hour',current_timestamp) -- reduce impact of incomplete hour
    group by 1
)

, before_after_metrics as (
    select 
        case 
            when hour <= timestamp '2024-03-14 14:50' then false
            else true
        end as after_dencun

        , avg(l1.num_txn) as l1_hourly_txns
        , avg(l1.hourly_gas_used) as l1_hourly_gas_usd
        , avg(l1.hourly_tx_fee) as l1_hourly_txfee

        , avg(l2.num_txn) as l2_hourly_txns
        , avg(l2.hourly_gas_used) as l2_hourly_gas_used
        , avg(l2.hourly_tx_fee)  as l2_hourly_txfee

        -- , avg(l2.hourly_tx_fee - l1.hourly_tx_fee) as "Hourly Profit"
        , median(l2.hourly_tx_fee - l1.hourly_tx_fee) as median_hourly_profit

        , avg(l2dex.hourly_trades) as l2_hourly_dex_trades
        , avg(l2lend.hourly_trades) as l2_hourly_lend_trades
        , avg(l2nft.hourly_trades) as l2_hourly_nft_trades
    from arbitrum_L1_fee l1 
    inner join arbitrum_L2_fee l2 using (hour)
    inner join dex_trades l2dex using (hour)
    inner join lending_trades l2lend using (hour)
    inner join nft_trades l2nft using (hour)
    group by 1
)

, rotate_before_metrics as (
    select 1 as rowidx, 'Hourly Txns on L1' as metric, l1_hourly_txns as "Before" from before_after_metrics where not after_dencun
    union all

    select 2 as rowidx, 'Hourly Usd Gas on L1' as metric, l1_hourly_gas_usd as "Before" from before_after_metrics where not after_dencun
    union all

    select 3 as rowidx, 'Hourly Fee on L1' as metric, l1_hourly_txfee as "Before" from before_after_metrics where not after_dencun
    union all

    select 4 as rowidx, 'Hourly Txns on L2' as metric, l2_hourly_txns as "Before" from before_after_metrics where not after_dencun
    union all

    select 5 as rowidx, 'Hourly Usd Gas on L2' as metric, l2_hourly_gas_used as "Before" from before_after_metrics where not after_dencun
    union all

    select 6 as rowidx, 'Hourly Fee on L2' as metric, l2_hourly_txfee as "Before" from before_after_metrics where not after_dencun
    union all

    select 7 as rowidx, 'Hourly Profit (Median)' as metric, median_hourly_profit as "Before" from before_after_metrics where not after_dencun
    union all

    select 8 as rowidx, 'Hourly DEX Trades on L2' as metric, l2_hourly_dex_trades as "Before" from before_after_metrics where not after_dencun
    union all

    select 9 as rowidx, 'Hourly Lending Trades on L2' as metric, l2_hourly_lend_trades as "Before" from before_after_metrics where not after_dencun
    union all

    select 10 as rowidx, 'Hourly NFT Trades on L2' as metric, l2_hourly_nft_trades as "Before" from before_after_metrics where not after_dencun
)

, rotate_after_metrics as (
    select 1 as rowidx, 'Hourly Txns on L1' as metric, l1_hourly_txns as "After" from before_after_metrics where after_dencun
    union all

    select 2 as rowidx, 'Hourly Usd Gas on L1' as metric, l1_hourly_gas_usd as "After" from before_after_metrics where after_dencun
    union all

    select 3 as rowidx, 'Hourly Fee on L1' as metric, l1_hourly_txfee as "After" from before_after_metrics where after_dencun
    union all

    select 4 as rowidx, 'Hourly Txns on L2' as metric, l2_hourly_txns as "After" from before_after_metrics where after_dencun
    union all

    select 5 as rowidx, 'Hourly Usd Gas on L2' as metric, l2_hourly_gas_used as "After" from before_after_metrics where after_dencun
    union all

    select 6 as rowidx, 'Hourly Fee on L2' as metric, l2_hourly_txfee as "After" from before_after_metrics where after_dencun
    union all

    select 7 as rowidx, 'Hourly Profit (Median)' as metric, median_hourly_profit as "After" from before_after_metrics where after_dencun
    union all

    select 8 as rowidx, 'Hourly DEX Trades on L2' as metric, l2_hourly_dex_trades as "After" from before_after_metrics where after_dencun
    union all

    select 9 as rowidx, 'Hourly Lending Trades on L2' as metric, l2_hourly_lend_trades as "After" from before_after_metrics where after_dencun
    union all

    select 10 as rowidx, 'Hourly NFT Trades on L2' as metric, l2_hourly_nft_trades as "After" from before_after_metrics where after_dencun
)

select 
    metric
    , "Before"
    , "After"
    , ""
from rotate_before_metrics
inner join rotate_after_metrics
    using (rowidx, metric)
order by rowidx
