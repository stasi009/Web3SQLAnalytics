-- https://dune.com/queries/3607426
with days_since_announce_ad as (
    -- https://twitter.com/Optimism/status/1760002821120983200
    -- airdrop is announced at 2024-02-21
    select date_diff('day', date '2024-02-21', current_date) as days
)

, nft_activities as (
    select
        blockchain 
        , block_date 
        , tx_hash 
        , buyer 
        , seller
        , amount_usd 
    from nft.trades

    union all 

    select
        blockchain 
        , block_date 
        , tx_hash 
        , buyer 
        , seller -- always zero address
        , amount_usd 
    from nft.mints
)

, daily_nft_trade as (
    select
        block_date
        , block_date < date '2024-02-21' as is_before_ad -- flag whether before announce airdrop

        , count(tx_hash)/2.0 as num_txns --/2是因为按buyer & seller各统计了一次，重复了
        , count(distinct tmp.trader)-1 as num_traders -- exclude 0x000，zero address在nft.mints中是seller
        , sum(amount_usd)/2.0 as trade_usd --/2是因为按buyer & seller各统计了一次，重复了
    from nft_activities
    cross join unnest(array[buyer, seller]) as tmp(trader)
    cross join days_since_announce_ad dsan
    where blockchain = '{{blockchain}}'
        and if('{{filter_project}}'='yes', project_contract_address = {{project_contract_address}}, true)
        -- 今天距离announce过去多少天，就自announce向前回溯多少天
        and block_time >= date_add('day', -1*dsan.days, date '2024-02-21') 
        and block_time < current_date -- avoid incomplete date
    group by 1,2
)

, medians as (
    select 
        is_before_ad
        , approx_percentile(num_traders, 0.5) as med_traders
        , approx_percentile(num_txns, 0.5) as med_txns
        , approx_percentile(trade_usd, 0.5) as med_trade_usd
    from daily_nft_trade
    group by 1
)

select 
    dmt.block_date 

    , if(is_before_ad, dmt.num_txns, null) as pread_txns
    , if(not is_before_ad, dmt.num_txns, null) as postad_txns
    , if(is_before_ad, md.med_txns, null) as pread_med_txns
    , if(not is_before_ad, md.med_txns, null) as postad_med_txns

    , if(is_before_ad, dmt.num_traders , null) as pread_traders
    , if(not is_before_ad, dmt.num_traders , null) as postad_traders
    , if(is_before_ad, md.med_traders , null) as pread_med_traders
    , if(not is_before_ad, md.med_traders , null) as postad_med_traders

    , if(is_before_ad, dmt.trade_usd , null) as pread_usd
    , if(not is_before_ad, dmt.trade_usd , null) as postad_usd
    , if(is_before_ad, md.med_trade_usd , null) as pread_med_usd
    , if(not is_before_ad, md.med_trade_usd , null) as postad_med_usd

from daily_nft_trade dmt
inner join medians md
    using (is_before_ad)

order by block_date

-- select * from "query_3607426(blockchain='ethereum',filter_project='no')"
-- select * from "query_3607426(blockchain='optimism',filter_project='no')"
-- select * from "query_3607426(blockchain='base',filter_project='no')"
-- select * from "query_3607426(blockchain='zora',filter_project='no')"