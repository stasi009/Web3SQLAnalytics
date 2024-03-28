with group_trades_by_user as (
    -- WHEN amount < 0 THEN 'short'
    -- WHEN amount > 0 THEN 'long'
    select 
        trader

        , sum(iff(amount>0, amount_usd, null)) as long_usd
        , sum(iff(amount>0, 1, null)) as long_trades

        , sum(iff(amount<0, amount_usd, null)) as short_usd
        , sum(iff(amount<0, 1, null)) as short_trades

        , sum(iff(amount>0,1,-1) * amount_usd) as net_position_usd

        , datediff('day', min(block_timestamp), current_date) as first_trade_tmdiff
        , datediff('day', max(block_timestamp), current_date) as last_trade_tmdiff
    from ARBITRUM.vertex.ez_perp_trades
    where is_taker 
        and symbol = 'BTC-PERP'
    group by 1
)

, short_bigwhale as (
    select * from group_trades_by_user
    order by net_position_usd asc -- bigwhale has largest short position
    limit 10 
)

, long_bigwhale as (
    select * from group_trades_by_user
    order by net_position_usd desc -- bigwhale has largest long position
    limit 10 
)

select * from short_bigwhale
union all 
select * from long_bigwhale


