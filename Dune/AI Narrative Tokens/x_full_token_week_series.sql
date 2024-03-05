with boundaries as (
    select 
        date_trunc('week', min(launch_date) ) as start_week
        , date_trunc('week',current_date) as end_week
    from query_3486591 -- ai token list
)

, ws as (
    select week
    from boundaries bd 
    -- sequence includes both ends
    cross join unnest(sequence(bd.start_week, bd.end_week, interval '7' day)) as weeks(week)
)

select 
    ws.week
    , ait.name as token_name
    , ait.symbol
    , ait.token_address
from query_3486591 ait -- ai token list
cross join ws -- 让每个token都有一条完整的时序


