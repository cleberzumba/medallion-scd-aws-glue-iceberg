-- =========================================================
-- validation_queries.sql
-- All Athena (Presto) queries used to validate the pipeline.
-- Run in the Athena query editor, workgroup "primary".
-- =========================================================

-- ---------------------------------------------------------
-- 1. Row counts across all 12 tables
-- ---------------------------------------------------------
SELECT 'bronze_raw_customers' AS table_name, COUNT(*) AS row_count FROM medallion_bronze.bronze_raw_customers
UNION ALL
SELECT 'bronze_raw_orders', COUNT(*) FROM medallion_bronze.bronze_raw_orders
UNION ALL
SELECT 'bronze_raw_products', COUNT(*) FROM medallion_bronze.bronze_raw_products
UNION ALL
SELECT 'silver_stg_customers', COUNT(*) FROM medallion_silver.silver_stg_customers
UNION ALL
SELECT 'silver_stg_products', COUNT(*) FROM medallion_silver.silver_stg_products
UNION ALL
SELECT 'silver_dim_customers_type1', COUNT(*) FROM medallion_silver.silver_dim_customers_type1
UNION ALL
SELECT 'silver_dim_customers_type2', COUNT(*) FROM medallion_silver.silver_dim_customers_type2
UNION ALL
SELECT 'silver_dim_products_type2', COUNT(*) FROM medallion_silver.silver_dim_products_type2
UNION ALL
SELECT 'silver_fact_orders', COUNT(*) FROM medallion_silver.silver_fact_orders
UNION ALL
SELECT 'gold_agg_sales_by_city', COUNT(*) FROM medallion_gold.gold_agg_sales_by_city
UNION ALL
SELECT 'gold_agg_sales_by_category', COUNT(*) FROM medallion_gold.gold_agg_sales_by_category
UNION ALL
SELECT 'gold_ranking_customers', COUNT(*) FROM medallion_gold.gold_ranking_customers;

-- ---------------------------------------------------------
-- 2. SCD2 integrity check: exactly one current version per key
--    (customers)
-- ---------------------------------------------------------
SELECT customer_id, COUNT(*) AS current_versions
FROM medallion_silver.silver_dim_customers_type2
WHERE is_current = true
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Same check for products
SELECT product_id, COUNT(*) AS current_versions
FROM medallion_silver.silver_dim_products_type2
WHERE is_current = true
GROUP BY product_id
HAVING COUNT(*) > 1;

-- ---------------------------------------------------------
-- 3. Current row must always have valid_to IS NULL
-- ---------------------------------------------------------
SELECT *
FROM medallion_silver.silver_dim_customers_type2
WHERE is_current = true
  AND valid_to IS NOT NULL;

SELECT *
FROM medallion_silver.silver_dim_products_type2
WHERE is_current = true
  AND valid_to IS NOT NULL;

-- ---------------------------------------------------------
-- 4. Referential integrity: orphan orders
--    (Presto has no LEFT ANTI JOIN, so this uses LEFT JOIN +
--    IS NULL on the right-hand key instead)
-- ---------------------------------------------------------
-- Orders with no matching current customer
SELECT f.order_id, f.customer_id
FROM medallion_silver.silver_fact_orders f
LEFT JOIN (
    SELECT customer_id FROM medallion_silver.silver_dim_customers_type2 WHERE is_current = true
) c ON f.customer_id = c.customer_id
WHERE c.customer_id IS NULL

UNION ALL

-- Orders with no matching current product
SELECT f.order_id, CAST(f.customer_id AS VARCHAR)
FROM medallion_silver.silver_fact_orders f
LEFT JOIN (
    SELECT product_id FROM medallion_silver.silver_dim_products_type2 WHERE is_current = true
) p ON f.product_id = p.product_id
WHERE p.product_id IS NULL;

-- ---------------------------------------------------------
-- 5. Point-in-time query example
--    "What was true about this dimension on a given date?"
-- ---------------------------------------------------------
SELECT *
FROM medallion_silver.silver_dim_customers_type2
WHERE valid_from <= DATE '2026-03-15'
  AND (valid_to > DATE '2026-03-15' OR valid_to IS NULL);

-- ---------------------------------------------------------
-- 6. Gold layer — business results
-- ---------------------------------------------------------
SELECT * FROM medallion_gold.gold_agg_sales_by_city ORDER BY total_amount DESC;

SELECT * FROM medallion_gold.gold_agg_sales_by_category ORDER BY total_amount DESC;

SELECT * FROM medallion_gold.gold_ranking_customers ORDER BY ranking;

-- ---------------------------------------------------------
-- 7. Idempotency check — batch control table
--    Confirms each batch key was processed exactly once,
--    even after re-running the Step Functions pipeline.
-- ---------------------------------------------------------
SELECT batch_key, COUNT(*) AS times_processed
FROM medallion_silver.silver_batch_control
GROUP BY batch_key
ORDER BY batch_key;

-- ---------------------------------------------------------
-- 8. Diagnostic: batch key extraction from _source_file
--    (used while debugging the Spark regex-escaping bug —
--    this version, run in Athena/Presto, worked correctly;
--    the equivalent Spark SQL needed doubled backslashes)
-- ---------------------------------------------------------
SELECT DISTINCT regexp_extract(_source_file, '(\d{4}-\d{2})', 1) AS batch_key
FROM medallion_bronze.bronze_raw_customers;
