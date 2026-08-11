-- ============================================================
-- Query 1: Average delivery time (in days) by customer state
-- ============================================================
-- Business question: which states have the slowest deliveries?
--
-- dim_tempo is joined twice (role-playing dimension), once to
-- resolve the purchase date and once to resolve the delivery
-- date, using DATEDIFF to compute the number of days between them.
--
-- Orders with no delivery date yet (order_delivery_date_id IS NULL)
-- are excluded, since there is no delivery time to measure for them.

SELECT
    dc.customer_state,
    AVG(DATEDIFF(day, t_compra.data_completa, t_entrega.data_completa)) AS tempo_medio_entrega_dias,
    COUNT(*) AS total_pedidos
FROM camada_gold.fato_vendas fv
JOIN camada_gold.dim_tempo t_compra
    ON fv.order_purchase_date_id = t_compra.data_id
JOIN camada_gold.dim_tempo t_entrega
    ON fv.order_delivery_date_id = t_entrega.data_id
JOIN camada_gold.dim_clientes dc
    ON fv.customer_id = dc.customer_id
WHERE fv.order_delivery_date_id IS NOT NULL
GROUP BY dc.customer_state
ORDER BY tempo_medio_entrega_dias DESC;