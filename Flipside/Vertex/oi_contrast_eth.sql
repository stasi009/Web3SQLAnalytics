with net_positions as (
    select 
        trader
        -- WHEN amount < 0 THEN 'short'
        -- WHEN amount > 0 THEN 'long'
        , sum(iff(amount>0,1,-1) * amount_usd) as net_position_usd
    from ARBITRUM.vertex.ez_perp_trades
    where is_taker 
        and symbol = 'ETH-PERP'
    group by 1
)

select 
    iff(net_position_usd>0, 'Long', 'Short') as position
    , sum(abs(net_position_usd)) as total_volume
    , count(trader) as num_holders
from net_positions
where abs(net_position_usd) >= 100
group by 1