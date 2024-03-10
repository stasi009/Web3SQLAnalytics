
-- with transfer as (
--     select 
--         "from"
--         , to   
--         , contract_address
--         , cast(value as double) / 1e18 as amount_adjdec -- both tokens have 18 decimals
--     from erc20_arbitrum.evt_transfer
--     where evt_block_time >= date '2024-01-24'
--         and contract_address in (
--             0xef888bcA6AB6B1d26dbeC977C455388ecd794794 -- Rari Governance Token (RGT)
--             , 0xCF8600347Dc375C5f2FdD6Dab9BB66e0b6773cd7 -- Rarible (RARI)
--         )
-- )

-- select * from transfer
