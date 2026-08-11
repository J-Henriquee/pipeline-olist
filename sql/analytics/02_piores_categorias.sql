-- ============================================================
-- Query 2: Top 5 product categories with the worst average review score
-- ============================================================
-- Business question: which product categories generate the most
-- customer dissatisfaction?
--
-- total_avaliacoes (review count) is included alongside the average
-- score on purpose: a category with only 1-2 reviews could show an
-- extreme average by chance, and this column lets the reader judge
-- how reliable each average actually is.

SELECT
    dp.product_category_name,
    AVG(da.review_score) AS nota_media,
    COUNT(*) AS total_avaliacoes
FROM camada_gold.fato_vendas fv
JOIN camada_gold.dim_produtos dp
    ON fv.product_id = dp.product_id
JOIN camada_gold.dim_avaliacoes da
    ON fv.order_id = da.order_id
GROUP BY dp.product_category_name
ORDER BY nota_media ASC
LIMIT 5;