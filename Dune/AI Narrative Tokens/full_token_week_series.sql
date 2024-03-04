with display_week_series as (
    select 
        week
    from unnest(sequence(
        date_trunc('week', date '2020-01-01'),
        date_trunc('week',current_date),
        interval '7' day
    )) as week_tbl(week)
)

select 
    ws.week
    , ait.name as token_name
    , ait.symbol
    , ait.token_address
from query_3486591 ait
cross join display_week_series ws
order by 1