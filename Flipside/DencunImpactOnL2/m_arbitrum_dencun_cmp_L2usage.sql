with dex_trades as (
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

        , avg(l2dex.hourly_trades) as l2_hourly_dex_trades
        , avg(l2lend.hourly_trades) as l2_hourly_lend_trades
        , avg(l2nft.hourly_trades) as l2_hourly_nft_trades
    from dex_trades l2dex 
    inner join lending_trades l2lend using (hour)
    inner join nft_trades l2nft using (hour)
    group by 1
)

, rotate_before_metrics as (
    select 8 as rowidx, 'Hourly DEX Trades on L2' as metric, l2_hourly_dex_trades as "Before" from before_after_metrics where not after_dencun
    union all

    select 9 as rowidx, 'Hourly Lending Trades on L2' as metric, l2_hourly_lend_trades as "Before" from before_after_metrics where not after_dencun
    union all

    select 10 as rowidx, 'Hourly NFT Trades on L2' as metric, l2_hourly_nft_trades as "Before" from before_after_metrics where not after_dencun
)

, rotate_after_metrics as (
    select 8 as rowidx, 'Hourly DEX Trades on L2' as metric, l2_hourly_dex_trades as "After" from before_after_metrics where after_dencun
    union all

    select 9 as rowidx, 'Hourly Lending Trades on L2' as metric, l2_hourly_lend_trades as "After" from before_after_metrics where after_dencun
    union all

    select 10 as rowidx, 'Hourly NFT Trades on L2' as metric, l2_hourly_nft_trades as "After" from before_after_metrics where after_dencun
)

select 
    metric
    , round("Before",2) as "Before"
    , round("After",2) as "After"
    , round(("After" / "Before" - 1)*100,2)::string || '%' as "Change Percentage %"
from rotate_before_metrics
inner join rotate_after_metrics
    using (rowidx, metric)
order by rowidx
