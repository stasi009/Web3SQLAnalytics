with days_since_announce_ad as (
    -- https://twitter.com/Optimism/status/1760002821120983200
    -- airdrop is announced at 2024-02-21
    select date_diff('day', date '2024-02-21', current_date) as days
)

, daily_nft_mint as (
    select
        block_date
        , block_date < date '2024-02-21' as is_before_ad -- flag whether before announce airdrop

        , blockchain
        , project_contract_address
        , project

        , count(tx_hash) as num_txns
        , count(distinct buyer) as num_minters -- buyers are mint to recipient
        , sum(amount_usd) as mint_cost_usd
    from nft.mints
    cross join days_since_announce_ad dsan
    where blockchain in ('optimism', 'ethereum', 'zora', 'base')
        -- 今天距离announce过去多少天，就自announce向前回溯多少天
        and block_time >= date_add('day', -1*dsan.days, date '2024-02-21') 
        and block_time < current_date -- avoid incomplete date
    group by 1,2,3,4,5
)

, each_contract_mint_across_ad as (
    select 
        blockchain
        , project_contract_address
        , project

        , count(block_date) filter (where is_before_ad) as pread_days
        , count(block_date) filter (where not is_before_ad) as postad_days
        
        , approx_percentile(num_minters, 0.5) filter (where is_before_ad) as pread_med_minterrs
        , approx_percentile(num_minters, 0.5) filter (where not is_before_ad) as postad_med_minters

        , approx_percentile(num_txns, 0.5) filter (where is_before_ad) as pread_med_txns
        , approx_percentile(num_txns, 0.5) filter (where not is_before_ad) as postad_med_txns

        , approx_percentile(mint_cost_usd, 0.5) filter (where is_before_ad) as pread_med_mint_usd
        , approx_percentile(mint_cost_usd, 0.5) filter (where not is_before_ad) as postad_med_mint_usd

    from daily_nft_mint
    group by 1,2,3
)

select 
    blockchain
    , project_contract_address
    , project

    , pread_days
    , postad_days

    , pread_med_minterrs
    , postad_med_minters
    , cast(postad_med_minters as double)/pread_med_minterrs - 1 as minters_chg_rate
    
    , pread_med_txns
    , postad_med_txns
    , cast(postad_med_txns as double)/pread_med_txns - 1 as mint_txn_chg_rate

    , pread_med_mint_usd
    , postad_med_mint_usd
    , postad_med_mint_usd/pread_med_mint_usd - 1 as mint_usd_chg_rate

from each_contract_mint_across_ad
where pread_days >= 30
    and postad_days >= 30
    
    and pread_med_minterrs > 0
    and pread_med_txns > 0
    and pread_med_mint_usd > 0
    
    and postad_med_minters > 0
    and postad_med_txns > 0
    and postad_med_mint_usd > 0

order by mint_usd_chg_rate desc