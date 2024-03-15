
with daily_prices as (
    select 
        date_trunc('day',minute) as block_date
        , contract_address 
        , avg(price) as avg_price   
    from prices.usd
    where blockchain = 'ethereum'
    group by 1,2
)

, daily_yield_report_with_usd as (
    select 
        yp.block_date
        , yp.yield_concurrency

        , sum(yp.yield) as daily_yield -- can be negative
        , sum(yp.yield * p.avg_price) as daily_yield_usd 

        , sum(yp.insurancePremiumPaid) as daily_insurance_paid
        , sum(yp.insurancePremiumPaid * p.avg_price) as daily_insurance_paid_usd

        , sum(yp.insuranceWithdrawn) as daily_insurance_withdraw  
        , sum(yp.insuranceWithdrawn * p.avg_price) as daily_insurance_withdraw_usd  
    from queries_3528338 yp -- sq_yield.sql
    inner join daily_prices p 
        on yp.block_date = p.block_date
        and yp.price_token = p.contract_address
    group by 1,2
)

select *  
from daily_yield_report_with_usd
