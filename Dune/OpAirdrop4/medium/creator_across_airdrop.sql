-- reference: https://dune.com/queries/3255065/
with eligible_nft_creators as (
    select * from dune.oplabspbc.result_superchain_nft_contracts_cleaned_opm_base_zora -- https://dune.com/queries/3181305
)
,daily_new_creators as (
    select 
        first_created_date
        ,count(creator_address) as num_creators
    from (
        select 
            creator_address
            ,min(date_trunc('day', created_time)) as first_created_date
        from eligible_nft_creators
        group by 1
    )
    group by 1
)
,growth as (
    select 
        *
    from (
        select 
            first_created_date
            ,case 
                when first_created_date between cast('2023-11-08' as timestamp) and cast('2024-01-08 23:00:00' as timestamp) then 'During ❤️'
                when first_created_date > cast('2024-01-08 23:00:00' as timestamp) then 'Post ❤️'
                else 'Before ❤️'
              end as period
            ,num_creators as first_time_creators
            ,sum(num_creators) over (order by first_created_date) as cumulative_num_creators
        from daily_new_creators
    )
    where 
        -- first_created_date >= date_trunc('day', now()) - interval '30' day
        first_created_date >= cast('2023-11-07' as timestamp) - interval '30' day
)
,starting_point as (
    select 
        max(case 
            when first_created_date = cast('2023-11-07' as timestamp) 
            then cumulative_num_creators end) as starting_point
    from growth
)
select 
    g.*
    ,coalesce(case 
        when first_created_date > cast('2023-11-07' as timestamp) 
        then g.cumulative_num_creators - starting_point
    end, 0) as incremental_creator_add
from growth as g
cross join starting_point
order by first_created_date desc