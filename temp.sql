WITH
  running_total_supply /* look at total supply over time */ 
  AS (
    WITH
      supply_actions AS (
        SELECT
          DATE_TRUNC('day', "evt_block_time") AS time,
          CASE
            WHEN t."from" = 0x0000000000000000000000000000000000000000 THEN 'minted'
            WHEN t."to" = 0x0000000000000000000000000000000000000000 THEN 'burned'
          END AS supply_action,
          CASE
            WHEN t."from" = 0x0000000000000000000000000000000000000000 THEN CAST(value AS DOUBLE)
            WHEN t."to" = 0x0000000000000000000000000000000000000000 THEN -1 * CAST(value AS DOUBLE)
          END AS token_value
        FROM
          erc20_{{Chain}}.evt_Transfer AS t
        WHERE
          "contract_address" = {{Token Address}}
          AND (
            t."from" = 0x0000000000000000000000000000000000000000
            OR t."to" = 0x0000000000000000000000000000000000000000
          )
      ),
      
      daily_supply AS (
        SELECT
          "time",
          SUM(CAST(token_value AS DOUBLE)/pow(10,COALESCE(tk.decimals,18))) AS daily_minted
        FROM
          supply_actions
        LEFT JOIN tokens.erc20 tk ON blockchain = 'ethereum' AND tk.contract_address = {{Token Address}}
        GROUP BY
          1
      )
      
    , total_supply as (
        SELECT
          "time",
          SUM("daily_minted") OVER (
            ORDER BY
              "time"
          ) AS total_supply
        FROM
          daily_supply ds 
      ) 
      
     SELECT 
     *
     FROM total_supply
  )
  
  , non_contract_balances AS (
    WITH
      tokens_sold AS (
        SELECT
          DATE_TRUNC('{{Granularity}}', tr."evt_block_time") AS time,
          tr."from",
          SUM(CAST(value AS DOUBLE)) AS value_sold
        FROM
          erc20_{{Chain}}.evt_Transfer AS tr
        WHERE
          tr."contract_address" = {{Token Address}}
        GROUP BY
          1,
          2
      ),
      tokens_bought AS (
        SELECT
          DATE_TRUNC('{{Granularity}}', tr."evt_block_time") AS time,
          tr."to",
          SUM(CAST(value AS DOUBLE)) AS value_bought
        FROM
          erc20_{{Chain}}.evt_Transfer AS tr
        WHERE
          tr."contract_address" = {{Token Address}}
        GROUP BY
          1,
          2
      ),
      daily_bought_sold AS (
        SELECT
          COALESCE(b."time", s."time") AS time,
          COALESCE(b."to", s."from") AS owner,
          COALESCE(CAST(value_bought AS DOUBLE), 0)/pow(10,COALESCE(tk.decimals,18)) AS bought,
          COALESCE(CAST(value_sold AS DOUBLE), 0)/pow(10,COALESCE(tk.decimals,18)) AS sold
        FROM
          tokens_bought AS b
          FULL OUTER JOIN tokens_sold AS s ON b."time" = s."time"
            AND b."to" = s."from"
          LEFT JOIN tokens.erc20 tk ON blockchain = 'ethereum' AND tk.contract_address = {{Token Address}}
      ),
      daily_balances AS (
        SELECT
          *,
          ROUND(
            SUM(bought - sold) OVER (
              PARTITION BY
                "owner"
              ORDER BY
                "time"
            ),
            3
          ) AS rolling_balance /* we round here because transfers w fees sometimes have rounding errors by 4th decimal */
        FROM
          daily_bought_sold
      )
    SELECT
      *
    FROM
      daily_balances AS db /* we don't want contracts in our balances */
    WHERE
      (
        (
            '{{Include Contracts}}' = 'include'
            --if not include, then remove contracts
            OR NOT EXISTS (
              SELECT
                1
              FROM
                {{Chain}}.creation_traces AS tr
              WHERE
                tr."address" = db."owner"
            )
        )
        --we want to include argent and gnosis safes
        OR EXISTS (
            SELECT 
            1
            FROM labels.contracts c 
            WHERE blockchain = '{{Chain}}'
            AND c.address = db.owner
            AND (lower(c.name) LIKE '%argent%' OR lower(c."name") LIKE '%aragon%' OR lower(c."name") LIKE '%daohaus%')
        )
        OR EXISTS ( 
          SELECT 
           1
          FROM safe_{{Chain}}.safes
          WHERE address = db.owner
        ) 
      )
      AND "owner" != 0x0000000000000000000000000000000000000000
  ),
  
  times AS (
        SELECT 
            *
        FROM query_2254711
        WHERE 'month' = '{{Granularity}}'
        AND time >= (SELECT min(time) FROM running_total_supply)
        AND time <= CAST('{{End Date}}' AS TIMESTAMP)      
        
        UNION ALL 
        
        SELECT 
            *
        FROM query_2254709
        WHERE 'week' = '{{Granularity}}'
        AND time >= (SELECT min(time) FROM running_total_supply)
        AND time <= CAST('{{End Date}}' AS TIMESTAMP)    
        
        UNION ALL 
        
        SELECT 
            *
        FROM query_2254698
        WHERE 'day' = '{{Granularity}}'
        AND time >= (SELECT min(time) FROM running_total_supply)
        AND time <= CAST('{{End Date}}' AS TIMESTAMP)
    ),
  
  owner_days /* cross join works for getting owner on each day, but it is slow (https://stackoverflow.com/questions/63130403/filling-missing-dates-in-each-group-while-querying-data-from-postgresql) */ 
  AS (
    SELECT DISTINCT
      d."time",
      "owner"
    FROM
      non_contract_balances
      CROSS JOIN times AS d
    GROUP BY
      1,
      2
  ),
  filled_owner_balances AS (
    SELECT
      *
    FROM
      (
        SELECT
          "time",
          "owner",
          FIRST_VALUE(rolling_balance) OVER (
            PARTITION BY
              "owner",
              grp_rolling_balance
          ) AS rolling_balance_final
        FROM
          (
            SELECT
              od."time",
              od."owner",
              n."rolling_balance",
              SUM(
                CASE
                  WHEN NOT n.rolling_balance IS NULL THEN 1
                END
              ) OVER (
                PARTITION BY
                  od."owner"
                ORDER BY
                  od."time"
              ) AS grp_rolling_balance
            FROM
              owner_days AS od
              LEFT JOIN non_contract_balances AS n ON n."time" = od."time"
              AND n."owner" = od."owner"
          ) AS a
      ) AS b
    WHERE
      NOT rolling_balance_final IS NULL
      AND rolling_balance_final > 0 /* filter from cross join section */
  ),
  
  supply_filled AS (
    SELECT
      "time",
      coalesce(total_supply, lag(total_supply, 1) IGNORE NULLS OVER (ORDER BY time asc)) as total_supply
    FROM
      (
        SELECT
          COALESCE(t."time", s.time) as time,
          total_supply
        FROM
          times AS t
          FULL OUTER JOIN running_total_supply AS s ON t."time" = s."time"
      ) AS p
  )
 
SELECT
  fb."time",
  s."total_supply",
  approx_percentile(rolling_balance_final, 0.5) AS "50th_percentile_holdings",
  approx_percentile(rolling_balance_final, 0.25) AS "25th_percentile_holdings",
  approx_percentile(rolling_balance_final, 0.75) AS "75th_percentile_holdings"
FROM
  filled_owner_balances AS fb
  LEFT JOIN supply_filled AS s ON s."time" = fb."time"
WHERE
  fb."time" >= CAST('{{Start Date}}' AS TIMESTAMP)
  AND fb."time" <= CAST('{{End Date}}' AS TIMESTAMP)
GROUP BY
  1,
  2