
with deposits as (
    select * 
    from query_3527756
    where block_date between current_date - interval '{{back_days}}' day and current_date - interval '1' day
)

, daily_prices as (
    select 
        date_trunc('day',minute) as block_date
        , contract_address 
        , avg(price) as avg_price   
    from prices.usd
    where blockchain = 'ethereum'
    group by 1,2
)

select 
    de.block_date
    , de.token
    , count(de.tx_hash) as num_deposit
    , approx_distinct(de.sender) as num_depositors
    , sum(de.amount) as deposit_amount
    , sum(de.amount * p.avg_price) as deposit_amount_usd
from deposits de
inner join daily_prices p
    on de.block_date = p.block_date 
    and de.price_token = p.contract_address
group by 1,2
order by 1,2