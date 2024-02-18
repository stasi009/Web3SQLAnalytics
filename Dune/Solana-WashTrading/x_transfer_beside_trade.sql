
with nft_trade_pairs as (
    select 
        tx_id,
        case 
            when buyer > seller then buyer || '-' || seller
            else seller || '-' || buyer 
        end trade_pair
    from nft_solana.trades
    where block_time >= now() - interval '{{backdays}}' day
),

token_transfer_pairs as (
    select 
        tx_id,
        case 
            when from_owner > to_owner then from_owner || '-' || to_owner
            else to_owner || '-' || from_owner 
        end transfer_pair
    from tokens_solana.transfers
    where action = 'transfer'
        and block_time >= now() - interval '{{backdays}}' day
)

select 
    nt.trade_pair,
    case 
        when tf.transfer_pair is null then 0 
        else 1
    end as transfer_beside_trade
from nft_trade_pairs nt
left join token_transfer_pairs tf
    on nt.trade_pair = tf.transfer_pair
    and nt.tx_id <> tf.transfer_pair

-- select 
--     transfer_beside_trade,
--     count(trade_pair) as num_pairs
-- from (
--     select 
--         nt.trade_pair,
--         case 
--             when tf.transfer_pair is null then 0 
--             else 1 
--         end as transfer_beside_trade
--     from nft_trade_pairs nt
--     left join token_transfer_pairs tf
--         on nt.trade_pair = tf.transfer_pair
-- )
-- group by transfer_beside_trade


