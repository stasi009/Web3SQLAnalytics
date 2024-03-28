
with liquidation as (
    select 
        date_trunc('day',lq.block_timestamp) as day 
        , case 
            when lq.amount_quote >0 then 'Good'
            else 'Bad' 
        end as "Liquidate Asset Quality"
        , lq.health_group_symbol as "Liquidate Token"
        , lq.amount_quote as "Liquidate USD" -- usd
        , trader as liquidatee
    from ARBITRUM.vertex.ez_liquidations lq 
    where block_timestamp >= current_date - interval '{{back_days}} day' 
        and block_timestamp < current_date -- avoid incomplete day
)

select 
    day
    , "Liquidate Asset Quality"
    , sum("Liquidate USD") as "Daily Liquidate USD"
from liquidation
group by 1,2
