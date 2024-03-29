
with weekly_liquidity_change as (
    select 
        date_trunc('week',block_timestamp) as week 
        , coalesce( sum(iff(amount>0, amount_usd, null)), 0 ) as deposit_usd
        , coalesce( sum(iff(amount<0, amount_usd, null)), 0 ) as withdraw_usd
    from ARBITRUM.vertex.ez_clearing_house_events
    where amount <> 0
    group by 1
)

select 
    week
    , deposit_usd
    , withdraw_usd -- negative
    , sum(deposit_usd + withdraw_usd) over (order by week) as total_liquidity
from weekly_liquidity_change
order by 1