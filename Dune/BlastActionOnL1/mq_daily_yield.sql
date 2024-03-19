-- https://dune.com/queries/3535165
with daily_prices as (
    select 
        date_trunc('day',minute) as block_date
        , contract_address 
        , avg(price) as daily_price   
    from prices.usd
    where blockchain = 'ethereum'
    group by 1,2
)

, day_currency_list as (
    select 
        *
        , case 
            when yield_currency='ETH' then 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2 -- use WETH to query price
            when yield_currency='USD' then 0x6B175474E89094C44Da98b954EedeAC495271d0F -- use Dai to query price
        end as price_token
    from (
        select 
            block_date
            , yield_currency 
        -- sequence includes both ends
        -- start day is when blast L1 bridge is deployed
        from unnest(sequence(date '2024-02-24', current_date - interval '1' day, interval '1' day)) as days(block_date)
        cross join UNNEST(ARRAY['ETH', 'USD']) AS currency_list(yield_currency)
    )
)

, daily_yield_report as (
    select 
        yp.block_date
        , yp.yield_currency

        , sum(yp.yield) as daily_yield -- can be negative
        , sum(yp.insurancePremiumPaid) as daily_insurance_paid
        , sum(yp.insuranceWithdrawn) as daily_insurance_withdraw  
    from query_3528338 yp -- sq_yield.sql
    group by 1,2
)

, daily_yield_report_fill_missing as (
    select 
        dcl.block_date
        , dcl.yield_currency

        , p.daily_price

        , coalesce(yp.daily_yield,0) as daily_yield
        , coalesce(yp.daily_yield,0) * p.daily_price  as daily_yield_usd

        , coalesce(yp.daily_insurance_paid,0) as daily_insurance_paid
        , coalesce(yp.daily_insurance_paid,0) * p.daily_price as daily_insurance_paid_usd

        , -1*coalesce(yp.daily_insurance_withdraw,0) as daily_insurance_withdraw
        , -1*coalesce(yp.daily_insurance_withdraw,0) * p.daily_price as daily_insurance_withdraw_usd
    from day_currency_list dcl
    left join daily_yield_report yp
        on dcl.block_date = yp.block_date
        and dcl.yield_currency = yp.yield_currency
    inner join daily_prices p 
        on dcl.block_date = p.block_date
        and dcl.price_token = p.contract_address
)

select 
    dyp.*

    , sum(daily_yield) over (partition by yield_currency order by block_date) as total_yield
    , (sum(daily_yield) over (partition by yield_currency order by block_date))*daily_price as total_yield_usd

    -- withdraw value has already turn to negative
    , sum(daily_insurance_paid + daily_insurance_withdraw) over (partition by yield_currency order by block_date) as total_insurance
    , (sum(daily_insurance_paid + daily_insurance_withdraw) over (partition by yield_currency order by block_date))*daily_price as total_insurance_usd

from daily_yield_report_fill_missing dyp
order by 1
