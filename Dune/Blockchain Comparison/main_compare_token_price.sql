
select
    date_trunc('day',minute) as day
    , symbol
    , case 
        when symbol='ETH' then 'ethereum / arbitrum / optimism'
        when symbol='AVAX' then 'avalanche_c'
        when symbol='MATIC' then 'polygon'
        when symbol='SOL' then 'solana'
        end as used_on_chain
    , avg(price) as daily_avg_price 
from prices.usd
where 
    blockchain is null -- for native token
    and symbol in ('ETH','SOL','AVAX','MATIC')
    and date_trunc('day',minute) between current_date - interval '{{back_days}}' day and current_date - interval '1' day
group by 1,2,3
order by 1,2