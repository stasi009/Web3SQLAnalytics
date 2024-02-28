
-- https://flipsidecrypto.github.io/solana-models/#!/model/model.solana_models.core__fact_events
-- https://flipsidecrypto.github.io/solana-models/#!/model/model.solana_models.core__fact_transactions

-- with magiceden_events as (
--     select 
--         block_timestamp,
--         tx_id,
--         index, -- identifies the event's position within a transaction
--         event_type,
--         instruction,
--         inner_instruction
--     from SOLANA.core.fact_events
--     where block_timestamp::date >= current_date - interval '1 day'
--         and program_id = 'M3mxk5W2tt27WGT7THox7PmgRDp4m6NEhL5xvxrBfS1'
--         and succeeded
-- )

-- select * from magiceden_events
-- order by block_timestamp desc
-- limit 1

select *  
from solana.core.fact_transactions
where block_timestamp::date >= current_date - interval '1 day'