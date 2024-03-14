
with L2proxyOnL1 as (
    SELECT
        value[0]::string as address
        , value[1]::string AS name
        , value[2]::string AS l2chain 
    FROM (
        SELECT
            livequery.live.udf_api('https://flipsidecrypto.xyz/api/queries/bca7fe3b-c929-4144-82e2-dc3378c3fd7a/latest-run') as response
        ), lateral FLATTEN (input => response:data:result:rows)
)