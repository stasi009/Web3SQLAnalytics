-- online url: https://dune.com/queries/3600731/
-- reference: https://dune.com/queries/3255065/
with eligible_nft_creators as (
    select 
        contract_address
        , token_standard
        , blockchain
        , created_time
        , creator_address
        , creation_tx_hash
        , is_first_time_flag -- creator第1次created nft
    from dune.oplabspbc.result_superchain_nft_contracts_cleaned_opm_base_zora -- https://dune.com/queries/3181305
)

,daily_new_creators as (
    select 
        first_created_date as day
        -- https://twitter.com/Optimism/status/1760002821120983200
        -- airdrop is announced at 2024-02-21
        , first_created_date < date '2024-02-21' as is_before_ad
        , count(creator_address) as new_creators
    from (
        select 
            creator_address
            , min(date_trunc('day', created_time)) as first_created_date
        from eligible_nft_creators
        group by 1
    )
    where first_created_date >= date '2024-02-21' - interval '30' day
    group by 1
)

, daily_new_creators_median as (
    select 
        is_before_ad
        , approx_percentile(new_creators, 0.5) as median_new_creators
    from daily_new_creators
    group by 1
)

select 
    day
    , if(is_before_ad, 'Before announce 🪂', 'After announce 🪂') as period

    , if(is_before_ad, new_creators, null )  as pread_new_creators
    , if(not is_before_ad, new_creators, null)  as postad_new_creators

    , if(is_before_ad, m.median_new_creators, null )  as pread_new_creators_median
    , if(not is_before_ad, m.median_new_creators, null)  as postad_new_creators_median

from daily_new_creators
inner join daily_new_creators_median m 
    using (is_before_ad)
order by 1