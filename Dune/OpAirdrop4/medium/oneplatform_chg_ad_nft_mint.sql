-- https://dune.com/queries/3607362
with days_since_announce_ad as (
    -- https://twitter.com/Optimism/status/1760002821120983200
    -- airdrop is announced at 2024-02-21
    select date_diff('day', date '2024-02-21', current_date) as days
)

, daily_nft_mint as (
    select
        block_date
        , block_date < date '2024-02-21' as is_before_ad -- flag whether before announce airdrop

        , count(tx_hash) as num_txns
        , count(distinct buyer) as num_minters -- buyers are mint to recipient
        , sum(amount_usd) as mint_cost_usd
    from nft.mints
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
        , approx_percentile(num_minters, 0.5) as med_minters
        , approx_percentile(num_txns, 0.5) as med_txns
        , approx_percentile(mint_cost_usd, 0.5) as med_mint_usd
    from daily_nft_mint
    group by 1
)

select 
    dmt.block_date 

    , if(is_before_ad, dmt.num_txns, null) as pread_txns
    , if(not is_before_ad, dmt.num_txns, null) as postad_txns
    , if(is_before_ad, md.med_txns, null) as pread_med_txns
    , if(not is_before_ad, md.med_txns, null) as postad_med_txns

    , if(is_before_ad, dmt.num_minters , null) as pread_minters
    , if(not is_before_ad, dmt.num_minters , null) as postad_minters
    , if(is_before_ad, md.med_minters , null) as pread_med_minters
    , if(not is_before_ad, md.med_minters , null) as postad_med_minters

    , if(is_before_ad, dmt.mint_cost_usd , null) as pread_usd
    , if(not is_before_ad, dmt.mint_cost_usd , null) as postad_usd
    , if(is_before_ad, md.med_mint_usd , null) as pread_med_usd
    , if(not is_before_ad, md.med_mint_usd , null) as postad_med_usd

from daily_nft_mint dmt
inner join medians md
    using (is_before_ad)

order by block_date