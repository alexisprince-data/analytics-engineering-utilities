-- cost_model_tests.sql
-- dbt-style assertions (zero rows = pass)

-- 1) Uniqueness of fact grain
SELECT plant_id, product_id, cost_date, COUNT(*) AS cnt
FROM fact_cost
GROUP BY plant_id, product_id, cost_date
HAVING COUNT(*) > 1;

-- 2) Not null keys
SELECT *
FROM fact_cost
WHERE plant_id IS NULL OR product_id IS NULL OR cost_date IS NULL;

-- 3) Referential integrity
SELECT fc.*
FROM fact_cost fc
LEFT JOIN dim_plant dp   ON dp.plant_id   = fc.plant_id
LEFT JOIN dim_product pr ON pr.product_id = fc.product_id
WHERE dp.plant_id IS NULL OR pr.product_id IS NULL;

-- 4) Non-negative costs (adjust if negatives are expected)
SELECT *
FROM fact_cost
WHERE material_cost < 0 OR labor_cost < 0 OR overhead_cost < 0;

-- 5) Date sanity (window as needed)
SELECT *
FROM fact_cost
WHERE cost_date < DATEADD(YEAR, -7, GETDATE());
