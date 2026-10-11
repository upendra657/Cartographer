-- Singular Test: Marts Core Reconciliation (Step 3.12)
-- Validates Kimball Invariants across the Marts layer:
-- 1. Financial parity: Total GMV (price) in fct_order_items equals stg_order_items (R$ 13,591,643.70) within R$ 0.01
-- 2. Seller-order grain count: fct_seller_orders row count equals exactly 100,010
-- 3. Monthly spine completeness: fct_seller_monthly_performance row count equals int_seller_month_spine (33,484)
-- 4. Status partition integrity: Sum of all seller lifecycle statuses equals total monthly rows (33,484)

WITH gmv_reconciliation AS (

    SELECT
        'GMV_MISMATCH' AS metric_name,
        ROUND(ABS(fct.total_fct_price - stg.total_stg_price), 2) AS diff
    FROM (
        SELECT SUM(price) AS total_fct_price
        FROM {{ ref('fct_order_items') }}
    ) fct
    CROSS JOIN (
        SELECT SUM(price) AS total_stg_price
        FROM {{ ref('stg_order_items') }}
    ) stg
    WHERE ROUND(ABS(fct.total_fct_price - stg.total_stg_price), 2) > 0.01

),

seller_orders_count_reconciliation AS (

    SELECT
        'SELLER_ORDERS_COUNT_MISMATCH' AS metric_name,
        ABS(COUNT(*) - 100010) AS diff
    FROM {{ ref('fct_seller_orders') }}
    HAVING COUNT(*) != 100010

),

monthly_spine_reconciliation AS (

    SELECT
        'MONTHLY_SPINE_COUNT_MISMATCH' AS metric_name,
        ABS(fct.cnt - spine.cnt) AS diff
    FROM (
        SELECT COUNT(*) AS cnt FROM {{ ref('fct_seller_monthly_performance') }}
    ) fct
    CROSS JOIN (
        SELECT COUNT(*) AS cnt FROM {{ ref('int_seller_month_spine') }}
    ) spine
    WHERE fct.cnt != spine.cnt OR fct.cnt != 33484

),

status_partition_integrity AS (

    SELECT
        'STATUS_PARTITION_INTEGRITY_MISMATCH' AS metric_name,
        ABS(COUNT(*) - SUM(CASE WHEN seller_lifecycle_status IN ('new', 'active', 'at_risk', 'churned', 'dormant', 'reactivated') THEN 1 ELSE 0 END)) AS diff
    FROM {{ ref('fct_seller_monthly_performance') }}
    HAVING COUNT(*) != SUM(CASE WHEN seller_lifecycle_status IN ('new', 'active', 'at_risk', 'churned', 'dormant', 'reactivated') THEN 1 ELSE 0 END)

),

unioned_failures AS (

    SELECT metric_name, diff FROM gmv_reconciliation
    UNION ALL
    SELECT metric_name, diff FROM seller_orders_count_reconciliation
    UNION ALL
    SELECT metric_name, diff FROM monthly_spine_reconciliation
    UNION ALL
    SELECT metric_name, diff FROM status_partition_integrity

)

SELECT * FROM unioned_failures
