-- https://dune.com/queries/3603530
with opchain_nft_creation as (
    select 
        date_trunc('day',created_time) as create_day
        , contract_address
        , blockchain
        , creator_address
        -- , is_first_time_flag -- 某creator在某blockchain上第1次create，并非全局第一次
    from dune.oplabspbc.result_superchain_nft_contracts_cleaned_opm_base_zora -- https://dune.com/queries/3181305
    where if('{{filter_chain}}' = 'yes', blockchain='{{blockchain}}',true)
)

, first_create_day as (
    select 
        creator_address
        , min(create_day) as first_create_day
    from opchain_nft_creation
    group by 1
)

, opchain_nft_creation_extend as (
    select 
        creator_address
        , create_day
        , blockchain 
        , contract_address
        , create_day = fd.first_create_day as is_firstcreate_day
    from opchain_nft_creation c
    inner join first_create_day fd 
        using (creator_address)
)

, days_since_announce_ad as (
    -- https://twitter.com/Optimism/status/1760002821120983200
    -- airdrop is announced at 2024-02-21
    select date_diff('day', date '2024-02-21', current_date) as days
)

, daily_creators as (
    select 
        create_day 
        -- https://twitter.com/Optimism/status/1760002821120983200
        -- airdrop is announced at 2024-02-21
        , create_day < date '2024-02-21' as is_before_ad

        , count(contract_address) as total_creations
        , count(contract_address) filter (where is_firstcreate_day) as creations_from_new_creators
    from opchain_nft_creation_extend 
    cross join days_since_announce_ad dsan
    where create_day >= date_add('day', -1*dsan.days, date '2024-02-21') 
        and create_day < current_date -- void incomplete date
    group by 1
)

, daily_creation_median as (
    select 
        is_before_ad
        , approx_percentile(total_creations, 0.5) as med_total_creations
        , approx_percentile(creations_from_new_creators, 0.5) as med_creations_from_new
    from daily_creators
    group by 1
)

select 
    create_day
    , if(is_before_ad, 'Before announce 🪂', 'After announce 🪂') as period

    , if(is_before_ad, total_creations, null )  as pread_total_creations
    , if(not is_before_ad, total_creations, null)  as postad_total_creations

    , if(is_before_ad, creations_from_new_creators, null )  as pread_creations_from_new_creators
    , if(not is_before_ad, creations_from_new_creators, null)  as postad_creations_from_new_creators

    , if(is_before_ad, m.med_total_creations, null )  as pread_med_total_creations
    , if(not is_before_ad, m.med_total_creations, null)  as postad_med_total_creations

    , if(is_before_ad, m.med_creations_from_new, null )  as pread_med_creations_from_new
    , if(not is_before_ad, m.med_creations_from_new, null)  as postad_med_creations_from_new

from daily_creators
inner join daily_creation_median m 
    using (is_before_ad)
order by 1

-- select * from "query_3603530(filter_chain='yes',blockchain='optimism')"
-- select * from "query_3603530(filter_chain='yes',blockchain='base')"
-- select * from "query_3603530(filter_chain='yes',blockchain='zora')"