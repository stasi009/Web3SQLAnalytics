
with ethereum_block_interval as (
    select 
        date_diff('second',b.time,nextb.time) as block_interval_secs
    from ethereum.blocks b
    inner join ethereum.blocks nextb
        on nextb.number = b.number + 1
        -- apply filter condition during inner join, reduce join data size
        and b.time >= now() - interval '{{back_days}}' day
        and nextb.time >= now() - interval '{{back_days}}' day
)
, avalanche_c_block_interval as (
    select 
        date_diff('second',b.time,nextb.time) as block_interval_secs
    from avalanche_c.blocks b
    inner join avalanche_c.blocks nextb
        on nextb.number = b.number + 1
        -- apply filter condition during inner join, reduce join data size
        and b.time >= now() - interval '{{back_days}}' day
        and nextb.time >= now() - interval '{{back_days}}' day
)
, arbitrum_block_interval as (
    select 
        date_diff('second',b.time,nextb.time) as block_interval_secs
    from arbitrum.blocks b
    inner join arbitrum.blocks nextb
        on nextb.number = b.number + 1
        -- apply filter condition during inner join, reduce join data size
        and b.time >= now() - interval '{{back_days}}' day
        and nextb.time >= now() - interval '{{back_days}}' day
)
, optimism_block_interval as (
    select 
        date_diff('second',b.time,nextb.time) as block_interval_secs
    from optimism.blocks b
    inner join optimism.blocks nextb
        on nextb.number = b.number + 1
        -- apply filter condition during inner join, reduce join data size
        and b.time >= now() - interval '{{back_days}}' day
        and nextb.time >= now() - interval '{{back_days}}' day
)
, polygon_block_interval as (
    select 
        date_diff('second',b.time,nextb.time) as block_interval_secs
    from polygon.blocks b
    inner join polygon.blocks nextb
        on nextb.number = b.number + 1
        -- apply filter condition during inner join, reduce join data size
        and b.time >= now() - interval '{{back_days}}' day
        and nextb.time >= now() - interval '{{back_days}}' day
)

select * from polygon_block_interval
limit 100