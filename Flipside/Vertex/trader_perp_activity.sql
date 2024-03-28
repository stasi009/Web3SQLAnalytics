
select 
    date_trunc('day',block_timestamp) as day
    , split(symbol,'-')[0] as asset
    , sum(iff(amount>0,1,-1) * amount_usd) as trade_volume
from ARBITRUM.vertex.ez_perp_trades
where is_taker 
    and trader = lower('{{trader}}')
group by 1,2
order by 1,2