-- ============================================================
-- Query 3: Month-over-month sales growth using the LAG window function
-- ============================================================
-- Business question: how is revenue trending month to month?
--
-- Step 1 (CTE faturamento_mensal): aggregates total revenue
-- (price + freight_value) per year-month, one row per month.
--
-- Step 2 (outer SELECT): uses LAG() to bring the previous month's
-- revenue into the same row as the current month, without collapsing
-- any rows — this is the key difference between a window function
-- and a GROUP BY aggregation.

WITH faturamento_mensal AS (
    SELECT
        t.ano,
        t.mes,
        SUM(fv.price + fv.freight_value) AS faturamento_total
    FROM camada_gold.fato_vendas fv
    JOIN camada_gold.dim_tempo t
        ON fv.order_purchase_date_id = t.data_id
    GROUP BY t.ano, t.mes
)
SELECT
    ano,
    mes,
    faturamento_total,
    LAG(faturamento_total) OVER (ORDER BY ano, mes) AS faturamento_mes_anterior,
    ROUND(
        (faturamento_total - LAG(faturamento_total) OVER (ORDER BY ano, mes))
        / LAG(faturamento_total) OVER (ORDER BY ano, mes) * 100,
        2
    ) AS crescimento_percentual
FROM faturamento_mensal
ORDER BY ano, mes;