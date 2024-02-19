with transfers_outof_trades as (
    with transfer_tx as (
        select distinct tx_id 
        from tokens_solana.transfers
        where action = 'transfer'
            and block_time >= now() - interval '{{backdays}}' day
    ),
    trade_tx as (
        select distinct tx_id 
        from nft_solana.trades
        where block_time >= now() - interval '{{backdays}}' day
    )
    select 
        f.tx_id as tx_id
    from transfer_tx f   
    left join trade_tx d 
        on f.tx_id = d.tx_id -- transfer happen during trade
    where d.tx_id is null -- transfer out of trade
)
select * from transfers_outof_trades