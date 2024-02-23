with fool as (
    select * from values 
        ('a','b',1,10,2),
        ('c','d',9,6,-1)
    as t(t1,t2,start_val,end_val,step)
)
select 
    ori.t1,
    ori.t2,
    expd.value
from fool as ori, 
lateral flatten(input => ARRAY_GENERATE_RANGE(start_val,end_val,step)) as expd