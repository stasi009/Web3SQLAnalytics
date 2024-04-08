-- online url: https://dune.com/queries/3600731/
with opchain_nft_creation as (
    select 
        date_trunc('day',created_time) as create_day
        , contract_address
        , blockchain
        , creator_address
        , is_first_time_flag -- creator第1次created nft
    from dune.oplabspbc.result_superchain_nft_contracts_cleaned_opm_base_zora -- https://dune.com/queries/3181305
)

, daily_creators as (
    select 
        create_day 
        -- https://twitter.com/Optimism/status/1760002821120983200
        -- airdrop is announced at 2024-02-21
        , create_day < date '2024-02-21' as is_before_ad

        , count(distinct contract_address) as total_creators
        , count(distinct contract_address) filter (where is_first_time_flag) as new_creators
    from opchain_nft_creation 
    where create_day >= date '2024-02-21' - interval '30' day
    group by 1

)

, daily_creators_median as (
    select 
        is_before_ad
        , approx_percentile(total_creators, 0.5) as median_total_creators
        , approx_percentile(new_creators, 0.5) as median_new_creators
    from daily_creators
    group by 1
)

select 
    create_day
    , if(is_before_ad, 'Before announce 🪂', 'After announce 🪂') as period

    , if(is_before_ad, total_creators, null )  as pread_total_creators
    , if(not is_before_ad, total_creators, null)  as postad_total_creators

    , if(is_before_ad, new_creators, null )  as pread_new_creators
    , if(not is_before_ad, new_creators, null)  as postad_new_creators

    , if(is_before_ad, m.median_total_creators, null )  as pread_total_creators_median
    , if(not is_before_ad, m.median_total_creators, null)  as postad_total_creators_median

    , if(is_before_ad, m.median_new_creators, null )  as pread_new_creators_median
    , if(not is_before_ad, m.median_new_creators, null)  as postad_new_creators_median

from daily_creators
inner join daily_creators_median m 
    using (is_before_ad)
order by 1