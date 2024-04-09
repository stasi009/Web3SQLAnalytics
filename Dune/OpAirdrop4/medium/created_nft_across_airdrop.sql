with opchain_nft_creation as (
    select 
        date_trunc('day',created_time) as create_day
        , contract_address
        , blockchain
        , creator_address
        -- , is_first_time_flag -- 某creator在某blockchain上第1次create，并非全局第一次
    from dune.oplabspbc.result_superchain_nft_contracts_cleaned_opm_base_zora -- https://dune.com/queries/3181305
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

, daily_creators as (
    select 
        create_day 
        -- https://twitter.com/Optimism/status/1760002821120983200
        -- airdrop is announced at 2024-02-21
        , create_day < date '2024-02-21' as is_before_ad

        , count(contract_address) as total_creations
        , count(contract_address) filter (where is_firstcreate_day) as creations_from_new_creators
    from opchain_nft_creation_extend 
    where create_day >= date '2024-02-21' - interval '30' day
        and create_day < current_date -- void incomplete date
    group by 1
)