
with dex_trades as (
    select 
        date_trunc('hour',block_timestamp) as hour 
        , count(tx_hash) as num_trades
        -- , sum(amount_in_usd + amount_out_usd)/2 as trade_volume
    from arbitrum.defi.ez_dex_swaps
    where txns.block_timestamp >= '2024-03-10'
        and txns.block_timestamp < date_trunc('hour',current_timestamp) -- reduce impact of incomplete hour
    group by 1
)

, lending_trades as (
    select 
        date_trunc('hour',block_timestamp) as hour 
        , count(tx_hash) as num_trades
        -- , sum(amount_usd) as trade_volume
    from arbitrum.defi.ez_lending_borrows
    where txns.block_timestamp >= '2024-03-10'
        and txns.block_timestamp < date_trunc('hour',current_timestamp) -- reduce impact of incomplete hour
    group by 1
)

, nft_trades as (
    select 
        date_trunc('hour',block_timestamp) as hour 
        , count(tx_hash) as num_trades -- 一次tx中可能有多笔交易，所以这里也不必去重
        -- , sum(price_usd) as trade_volume
    from arbitrum.nft.ez_nft_sales
    where txns.block_timestamp >= '2024-03-10'
        and txns.block_timestamp < date_trunc('hour',current_timestamp) -- reduce impact of incomplete hour
    group by 1
)

select 
    hour

    , case 
        when hour <= timestamp '2024-03-13 13:55:59' then 'Before Dencun'
        when hour <= timestamp '2024-03-14 14:50' then 'Not Active On Arbitrum'
        else 'Dencun Active On Arbitrum'
    end as dencun_flag

    , dex.num_trades as dex_trades 
    , lend.num_trades as lend_trades 
    , nft.num_trades as nft_trades
from dex_trades dex
inner join lending_trades lend 
    using (hour)
inner join nft_trades nft 
    using (hour)
order by hour