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

select 
    case 
        when hour <= timestamp '2024-03-14 14:50' then 'Before Active On Arbitrum'
        else 'Dencun Active On Arbitrum'
    end as dencun_flag

    , avg(l1.num_txn) as "Hourly Txns on L1"
    , avg(l1.hourly_gas_used) as "Hourly Usd Gas on L1"
    , avg(l1.hourly_tx_fee) as "Hourly Fee on L1"

    , avg(l2.num_txn) as "Hourly Avg Txns on L2"
    , avg(l2.hourly_gas_used) as "Hourly Usd Gas on L2"
    , avg(l2.hourly_tx_fee)  as "Hourly Fee on L2"

    -- , avg(l2.hourly_tx_fee - l1.hourly_tx_fee) as "Hourly Profit"
    , median(l2.hourly_tx_fee - l1.hourly_tx_fee) as "Hourly Profit (Median)"

    , avg(l2dex.hourly_trades) as "Hourly DEX Trades on L2"
    , avg(l2lend.hourly_trades) as "Hourly Lending Trades on L2"
    , avg(l2nft.hourly_trades) as "Hourly NFT Sale Trades on L2"
from arbitrum_L1_fee l1 
inner join arbitrum_L2_fee l2 using (hour)
inner join dex_trades l2dex using (hour)
inner join lending_trades l2lend using (hour)
inner join nft_trades l2nft using (hour)
group by 1