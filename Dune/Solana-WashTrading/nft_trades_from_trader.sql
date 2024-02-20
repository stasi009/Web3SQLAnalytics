
select *
from query_3445248 q 
where q.buyer = '{{account}}'
or q.seller = '{{account}}'
order by block_time