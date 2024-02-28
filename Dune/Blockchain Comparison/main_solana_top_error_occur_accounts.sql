
with account_n_success as (
    select 
        accounts.account
        , success
    from solana.transactions as txn
    cross join unnest(account_keys) as accounts(account)
    where txn.block_time >= now() - interval '1' day
        and accounts.account not in (
            '11111111111111111111111111111111' -- System Program
            , 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA' -- Token Program
            , 'So11111111111111111111111111111111111111112' -- Wrapped SOL
            , 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL' -- Associated Token Account Program
        )
)

, high_fail_accounts as (
    select 
        account
        , total_txns
        , total_succ_txns
        , cast(total_succ_txns as double) / total_txns as success_rate
    from (
        select 
            account
            , count(success) as total_txns
            , count(success) filter (where success) as total_succ_txns
        from account_n_success
        group by 1
    )
    where total_txns >= 100000
        and cast(total_succ_txns as double) / total_txns < 0.5 -- only care about those high fail rate
)

, program_names as (
    select distinct  namespace,
                    program_name,
                    program_id
    from solana.discriminators
)

select 
    hfa.account
    , pn.namespace
    , pn.program_name
    , hfa.total_txns
    , hfa.total_succ_txns
    , hfa.success_rate
from high_fail_accounts hfa
left join program_names pn 
    on hfa.account = pn.program_id
order by success_rate
