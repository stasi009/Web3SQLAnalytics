with sell_op_token as (
    select 
        block_time 
        , block_date

        , taker as seller
        
        , project_contract_address
        , project

        , amount_usd
        , token_sold_amount -- has already adjusted decimals

        , token_bought_address 
        , token_bought_symbol
    from dex.trades
    where blockchain = 'optimism'
        and token_sold_address = 0x4200000000000000000000000000000000000042 -- OP Token
        and block_date >= date '2024-01-01' -- limit time range to reduce data size
)
