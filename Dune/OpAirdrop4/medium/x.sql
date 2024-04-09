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

-- select 
--     count(creator_address) -- 530
-- from first_create_day
-- where first_create_day = date '2024-01-22'

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

-- select 
--     count(distinct contract_address) as total_creators -- 1046
--     , count(distinct contract_address) filter (where is_firstcreate_day) as new_creators -- 631
-- from opchain_nft_creation_extend
-- where create_day = date '2024-01-22'

, temp1 as (
    select 
        distinct contract_address as contract_address
    from opchain_nft_creation_extend
    where create_day = date '2024-01-22'
        and is_firstcreate_day
)

select 
    creator_address
    , fc.first_create_day
from temp1 t
left join first_create_day fc
    using (creator_address)
