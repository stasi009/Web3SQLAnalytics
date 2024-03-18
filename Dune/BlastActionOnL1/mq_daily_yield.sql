
with daily_prices as (
    select 
        date_trunc('day',minute) as block_date
        , contract_address 
        , avg(price) as avg_price   
    from prices.usd
    where blockchain = 'ethereum'
    group by 1,2
)

, day_concurrency_list as (
    select 
        block_date
        , yield_concurrency 
    -- sequence includes both ends
    -- start day is when blast L1 bridge is deployed
    from unnest(sequence(date '2024-02-24', current_date - interval '1' day, interval '1' day)) as days(block_date)
    cross join UNNEST(ARRAY['ETH', 'USD']) AS concurrency_list(yield_concurrency)
)

, daily_yield_report as (
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

, daily_yield_report_fill_missing as (
    select 
        block_date
        , yield_concurrency

        , coalesce(avg_price,0) as avg_price

        , coalesce(daily_yield,0) as daily_yield
        , coalesce(daily_yield_usd,0) as daily_yield_usd

        , coalesce(daily_insurance_paid,0) as daily_insurance_paid
        , coalesce(daily_insurance_paid_usd,0) as daily_insurance_paid_usd

        , -1*coalesce(daily_insurance_withdraw,0) as daily_insurance_withdraw
        , -1*coalesce(daily_insurance_withdraw_usd,0) as daily_insurance_withdraw_usd
    from day_concurrency_list dcl
    left join daily_yield_report yp
        using (block_date, yield_concurrency)
)

select 
    dp.*

    , sum(daily_yield) over (partition by yield_concurrency order by block_date) as total_yield
    , (sum(daily_yield) over (partition by yield_concurrency order by block_date))*avg_price as total_yield_usd

    -- withdraw value has already turn to negative
    , sum(daily_insurance_paid + daily_insurance_withdraw) over (partition by yield_concurrency order by block_date) as total_insurance
    , (sum(daily_insurance_paid + daily_insurance_withdraw) over (partition by yield_concurrency order by block_date))*avg_price as total_insurance_usd

from daily_yield_report_fill_missing dyp
order by 1
