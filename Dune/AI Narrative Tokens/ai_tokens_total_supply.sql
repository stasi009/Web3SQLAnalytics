
with weekly_mint as (
    select 
        date_trunc('week',block_time) as week
        , token_name
        , token_address
        , sum(value_adjdec) as mint_val_adjdec
    from query_3486854 as ait -- ai token transfer
    where ait."from" = 0x0000000000000000000000000000000000000000
    group by 1,2,3
)

, weekly_burn as (
    select 
        date_trunc('week',block_time) as week
        , token_name
        , token_address
        , sum(value_adjdec) as burn_val_adjdec
    from query_3486854 as ait -- ai token transfer
    where ait.to = 0x0000000000000000000000000000000000000000
    group by 1,2,3
)

select 
    mint.week 
    , mint.token_name
    , mint.token_address
    , mint.mint_val_adjdec
    , burn.burn_val_adjdec
    , (mint.mint_val_adjdec - burn.burn_val_adjdec) as net_supply
from weekly_mint mint 
inner join weekly_burn burn 
    using (week, token_name, token_address)
