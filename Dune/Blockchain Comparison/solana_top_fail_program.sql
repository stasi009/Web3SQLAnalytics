
with program_n_success as (
    select 
        ixns.program_id
        , success
    from solana.transactions as txn
    cross join unnest(instructions) as ixns(data, program_id, args, inner_ixns)
    where txn.block_time >= now() - interval '1' day
        and ixns.program_id not in (
            '11111111111111111111111111111111' -- System Program
            , 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA' -- Token Program
            , 'So11111111111111111111111111111111111111112' -- Wrapped SOL
            , 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL' -- Associated Token Account Program
            , 'ComputeBudget111111111111111111111111111111' -- set the compute unit limit and compute unit price
        )
)

, high_fail_programs as (
    select 
        program_id
        , total_txns
        , total_succ_txns
        , cast(total_succ_txns as double) / total_txns as success_rate
    from (
        select 
            program_id
            , count(success) as total_txns
            , count(success) filter (where success) as total_succ_txns
        from program_n_success
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
    hfp.program_id
    , pn.namespace
    , pn.program_name
    , hfp.total_txns
    , hfp.total_succ_txns
    , hfp.success_rate
from high_fail_programs hfp
left join program_names pn 
    on hfp.program_id = pn.program_id
order by success_rate
