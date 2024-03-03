with withdraw as 
  (
  select 
  block_timestamp,
  origin_function_signature,
  tx_hash,
  origin_from_address as usr,origin_to_address,
  'withdraw' as act,
  concat('0x',substr(topics[2],27,40)) as currency,
   tokenflow_eth.hextoint(concat('0x',substr(data,27,40)))*-1/1e18 as amount
  from ethereum.core.fact_event_logs
  where topics[0] ='0x3ed4ee04a905a278b050a856bbe7ddaaf327a30514373e65aa6103beeae488c3'
  and contract_address in ('0x70b97a0da65c15dfb0ffa02aee6fa36e507c2762','0x5f6ac80CdB9E87f3Cfa6a90E5140B9a16A361d5C')
and origin_to_address in ('0x3b968d2d299b895a5fcf3bba7a64ad0f566e6f88','0x2338d34337dd0811b684640de74717b73f7b8059')
  -- and block_timestamp > current_date - interval ' 1 month'
  ),
deposits 
  as 
  (
  select block_timestamp,
  origin_function_signature,
  tx_hash,
  origin_from_address as usr,origin_to_address,
  'deposit' as act,
  concat('0x',substr(topics[1],27,40)) as currency,
   tokenflow_eth.hextoint(concat('0x',substr(data,27+64,40)))/1e18 as amount
  from ethereum.core.fact_event_logs
  where topics[0] ='0x443ff2d25883a4800d36062db52ca3dd7ced05bd8627c8a6a37f8699715b5431'
  and contract_address in ('0x70b97a0da65c15dfb0ffa02aee6fa36e507c2762','0x5f6ac80CdB9E87f3Cfa6a90E5140B9a16A361d5C')
and origin_to_address in ('0x3b968d2d299b895a5fcf3bba7a64ad0f566e6f88','0x3710d54de90324c8ba4b534d1e3f0fcedc679ca4')
    -- and block_timestamp > current_date - interval ' 1 month'

  ),
combined_reserve as 
  (
  select * from withdraw 
  union 
  select * from deposits
  ),
borrow as 
  (
  select 
  'borrow' as act,
  block_timestamp, 
  tx_hash,
  origin_function_signature,
  origin_from_address as usr,
  event_index,
  tokenflow_eth.hextoint(concat('0x',substr(data,27+64,40)))/1e18 as amount,
  concat('0x',substr(data,27+64+64,40)) as nft,
  tokenflow_eth.hextoint(concat('0x',substr(data,27+64+64+64,40))) as token_id,
  -- tokenflow_eth.hextoint(concat('0x',substr(data,27+64+64+64+64,40))) as borrow_rate,
  tokenflow_eth.hextoint(concat('0x',substr(data,27+64+64+64+64+64,40))) as loan_id
  from ethereum.core.fact_event_logs
  where topics[0]='0xcfb3a669117d9dc90f0d3521228dc9fe67c5102dde205ef16fe9a1f81be698d5'
and origin_to_address='0x3b968d2d299b895a5fcf3bba7a64ad0f566e6f88'
  and origin_function_signature in ('0x9c748eff','0x35611f7e','0xd0554fc6','0xf8a3310b')
  ),
repay as 
  (
  select 
  'repay' as act,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address as usr, 
  event_index,
  tokenflow_eth.hextoint(concat('0x',substr(data,27+64,40)))*-1/1e18 as amount,
  concat('0x',substr(topics[2],27,40)) as nft,
  tokenflow_eth.hextoint(concat('0x',substr(data,27+64+64,40))) as token_id,
  tokenflow_eth.hextoint(concat('0x',substr(data,27+64+64+64,40))) as loan_id
  from ethereum.core.fact_event_logs
  where topics[0]='0x50e03867c1178391f204f7bf0eb2f52d5167dc65a99a9650a95abe55d17be17e'
and  origin_function_signature in ('0x89cbe656','0x5a953999','0x633d2a19','0x7f185c1e')
  and origin_to_address in ('0x3b968d2d299b895a5fcf3bba7a64ad0f566e6f88',lower('0xeD01f8A737813F0bDA2D4340d191DBF8c2Cbcf30'))
  ),
  liq as 
  (
  select 
  'liquidate' as act,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address as usr,
  event_index,
  tokenflow_eth.hextoint(concat('0x',substr(data,27+64,40)))*-1/1e18 as bid_amount,
  concat('0x',substr(topics[2],27,64)) as nft,
  tokenflow_eth.hextoint(concat('0x',substr(data,27+64+64+64+64,40))) as token_id,
  tokenflow_eth.hextoint(concat('0x',substr(data,27+64+64+64+64+64,40))) as loan_id
from ethereum.core.fact_event_logs
  where
  topics[0]='0xf028795898a18c6fc88094dc5671c6a79d5dc3458c44015e9299fbc6c6268cf8'
  and origin_to_address='0x3b968d2d299b895a5fcf3bba7a64ad0f566e6f88'
  ),
  redeem as 
  (
  select 
  'redeem' as act
  ,block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address as usr,
  event_index,
  tokenflow_eth.hextoint(concat('0x',substr(data,27+64,40)))*-1/1e18 as amount,
  concat('0x',substr(topics[2],27,64)) as nft,
  -- tokenflow_eth.hextoint(concat('0x',substr(data,27+64,40)))/1e18 as borrow_amount,
   -- tokenflow_eth.hextoint(concat('0x',substr(data,27+64+64,40)))/1e18 as extra_amount,
  tokenflow_eth.hextoint(concat('0x',substr(data,27+64+64+64,40))) as token_id,
  tokenflow_eth.hextoint(concat('0x',substr(data,27+64+64+64+64,40))) as loan_id
from ethereum.core.fact_event_logs
  where
  topics[0]='0x0fcfe1a3f2afab13e32fa3c091795159ed5dfe66dc078e21c7f521f42e163afc'
  and origin_to_address='0x3b968d2d299b895a5fcf3bba7a64ad0f566e6f88'
  ),
combined_debt as 
(
select * from borrow 
  union 
  select * from repay
  union 
  select * from liq
  union 
  select * from redeem
  ),
tab1 as 
(
  select block_timestamp::date as "date",sum(amount) as debt,sum(debt) over(order by "date" asc) as cummulative_debt from combined_debt
  
group by "date"
order by "date" desc
),
tab2 as 
(
  select block_timestamp::date as "date", sum(amount) as reserve, sum(reserve) over(order by "date" asc) as cum_reserve from combined_reserve

group by "date"
order by "date" desc 
),
apr as
  (
    select block_timestamp::date as "date",
    avg(trunc(((ethereum.public.udf_hex_to_int(substring(data,45,22))::integer)/1e25),2))as Deposit_APR
    from ethereum.core.fact_event_logs
    where topics[0] = lower('0x4063a2df84b66bb796eb32622851d833e57b2c4292900c18f963af8808b13e35')
    and contract_address = lower('0x70b97a0da65c15dfb0ffa02aee6fa36e507c2762')
    and block_timestamp > current_date - interval ' 1 month'
    group by "date"
    order by "date" DESC
  )
select 
  tab1."date", 
  tab1.cummulative_debt,
  tab2.cum_reserve,
  tab1.cummulative_debt*100/tab2.cum_reserve as utlisiation_factor, 
  apr.Deposit_APR
from tab1 inner join tab2 
on tab1."date"=tab2."date" left join apr on tab1."date"=apr."date" 
where tab1."date" > current_date - interval ' 1 month'