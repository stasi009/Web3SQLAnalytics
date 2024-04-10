-- https://dune.com/queries/3608267
with days_since_announce_ad as (
    -- https://twitter.com/Optimism/status/1760002821120983200
    -- airdrop is announced at 2024-02-21
    select date_diff('day', date '2024-02-21', current_date) as days
)

, daily_sell_op as (
    select 
        block_date
        , block_date < date '2024-02-21' as is_before_ad -- flag whether before announce airdrop

        , count(distinct taker) as daily_sellers
        , sum(token_sold_amount) as daily_sold_op
        , sum(amount_usd) / sum(token_sold_amount) as daily_op_price

    from dex.trades
    cross join days_since_announce_ad dsan
    where blockchain = 'optimism'
        and token_sold_address = 0x4200000000000000000000000000000000000042 -- OP Token
        -- 今天距离announce过去多少天，就自announce向前回溯多少天
        and block_time >= date_add('day', -1*dsan.days, date '2024-02-21') 
        and block_time < current_date -- avoid incomplete date

    group by 1,2
)

, medians as (
    select 
        is_before_ad
        , approx_percentile(daily_sellers, 0.5) as med_sellers
        , approx_percentile(daily_sold_op, 0.5) as med_sold_op
        , approx_percentile(daily_op_price, 0.5) as med_op_price
    from daily_sell_op
    group by is_before_ad
)

select 
    s.block_date

    , if(is_before_ad, daily_sellers, null) as pread_sellers
    , if(is_before_ad, daily_sold_op, null) as pread_sold_op
    , if(is_before_ad, daily_op_price, null) as pread_price

    , if(is_before_ad, m.med_sellers, null) as pread_med_sellers
    , if(is_before_ad, m.med_sold_op, null) as pread_med_sold_op
    , if(is_before_ad, m.med_op_price, null) as pread_med_price

    , if(not is_before_ad, daily_sellers, null) as postad_sellers
    , if(not is_before_ad, daily_sold_op, null) as postad_sold_op
    , if(not is_before_ad, daily_op_price, null) as postad_price

    , if(not is_before_ad, m.med_sellers, null) as postad_med_sellers
    , if(not is_before_ad, m.med_sold_op, null) as postad_med_sold_op
    , if(not is_before_ad, m.med_op_price, null) as postad_med_price


from daily_sell_op s   
inner join medians m 
    using (is_before_ad)
order by 1
