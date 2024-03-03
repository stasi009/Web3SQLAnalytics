WITH Ranked_Revenue AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as order_col, -- Preserves original order
        cat,
        revenue_2023,
        revenue_2022,
        perc_change
    FROM query_3399354
),
Expenses AS (
    WITH 
        funding_2023 AS (
            SELECT 
                '5.2 Funding Expenses' as cat,
                SUM(a.expenses) as expenses_2023
            FROM query_3404375 a
        ),
        funding_2022 AS (
            SELECT 
                '5.2 Funding Expenses' as cat,
                SUM(b.expenses) as expenses_2022
            FROM query_3404377 b
        )
    SELECT 
        a.cat,
        expenses_2023,
        expenses_2022,
        (expenses_2023 - expenses_2022) / NULLIF(expenses_2022, 0) AS perc_change
    FROM funding_2023 a
    LEFT JOIN funding_2022 b ON a.cat = b.cat
),
Final_Results AS (
    SELECT 
        order_col,
        cat,
        revenue_2023,
        revenue_2022,
        perc_change
    FROM (
        SELECT 0 as order_col, '4 INCOME' as cat, NULL as revenue_2023, NULL as revenue_2022, NULL as perc_change
        UNION ALL
        SELECT * FROM Ranked_Revenue
        UNION ALL
        SELECT 5.5 as order_col, '5 EXPENSES' as cat, NULL as revenue_2023, NULL as revenue_2022, NULL as perc_change
        UNION ALL
        SELECT 6.5 as order_col, cat, expenses_2023, expenses_2022, perc_change FROM Expenses
    ) AS Revenue_With_Title
)

SELECT
    order_col,
    cat,
    CASE
        WHEN cat = '--- Total Income ---' THEN 276504636.76
        WHEN cat = '--- Total Expenses ---' THEN 70850248.57
        WHEN cat = '--- NET INCOME ---' THEN 276504636.76 - 70850248.5755416
        ELSE revenue_2023
    END AS revenue_2023,
    CASE
        WHEN cat = '--- Total Income ---' THEN 585616519.19
        WHEN cat = '--- Total Expenses ---' THEN 42731822.80
        WHEN cat = '--- NET INCOME ---' THEN 585616519.19 - 42731822.80
        ELSE revenue_2022
    END AS revenue_2022,
    perc_change
FROM Final_Results
ORDER BY order_col;
