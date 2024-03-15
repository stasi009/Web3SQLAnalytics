
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

        , avg(p.avg_price) as avg_price -- 其实一天之内一个yield_concurrency所有avg_price肯定相同，这里再取avg只不过为了取唯一

        , sum(yp.yield) as daily_yield -- can be negative
        , sum(yp.yield * p.avg_price) as daily_yield_usd 

        , sum(yp.insurancePremiumPaid) as daily_insurance_paid
        , sum(yp.insurancePremiumPaid * p.avg_price) as daily_insurance_paid_usd

        , sum(yp.insuranceWithdrawn) as daily_insurance_withdraw  
        , sum(yp.insuranceWithdrawn * p.avg_price) as daily_insurance_withdraw_usd  
    from query_3528338 yp -- sq_yield.sql
    inner join daily_prices p 
        on yp.block_date = p.block_date
        and yp.price_token = p.contract_address
    group by 1,2
)

select 
    block_date
    , yield_concurrency
    , avg_price

    , daily_yield -- can be negative
    , daily_yield_usd 
    , sum(daily_yield) over (partition by yield_concurrency order by block_date) as total_yield
    , (sum(daily_yield) over (partition by yield_concurrency order by block_date))*avg_price as total_yield_usd

    , daily_insurance_paid
    , daily_insurance_paid_usd

    , -1*daily_insurance_withdraw as daily_insurance_withdraw
    , -1*daily_insurance_withdraw_usd as daily_insurance_withdraw_usd

    , sum(daily_insurance_paid - daily_insurance_withdraw) over (partition by yield_concurrency order by block_date) as total_insurance
    , (sum(daily_insurance_paid - daily_insurance_withdraw) over (partition by yield_concurrency order by block_date))*avg_price as total_insurance_usd

from daily_yield_report_with_usd dyp
order by 1
