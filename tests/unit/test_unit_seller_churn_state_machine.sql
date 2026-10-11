-- Unit Test: Seller Churn Transition State Machine (Step 3.10)
-- Validates:
-- 1. Month 1: First sale -> 'new'
-- 2. Month 2: Active sale -> 'active'
-- 3. Consecutive inactive months:
--    - 1-2 inactive months -> 'at_risk'
--    - 3rd inactive month -> 'churned'
--    - 4th+ inactive months -> 'dormant'
-- 4. Sale resumption after 3+ inactive months -> 'reactivated'
-- 5. Sale resumption after 1-2 inactive months -> 'active' (not reactivated)
-- Returns 0 rows on pass.

WITH mock_seller_history AS (

    -- Mock Seller A (Contiguous Lifecycle Progression):
    -- Month 1 (Sale) -> 'new'
    -- Month 2 (Sale) -> 'active'
    -- Month 3 (No sale, 1st inactive month) -> 'at_risk'
    -- Month 4 (No sale, 2nd inactive month) -> 'at_risk'
    -- Month 5 (No sale, 3rd inactive month) -> 'churned'
    -- Month 6 (No sale, 4th inactive month) -> 'dormant'
    -- Month 7 (Sale resumes after 3+ inactive months) -> 'reactivated'
    SELECT 'MOCK_SELLER_A' AS seller_id, '2017-01-01'::DATE AS month, TRUE AS is_active, 'new' AS expected_lifecycle_status UNION ALL
    SELECT 'MOCK_SELLER_A', '2017-02-01'::DATE, TRUE, 'active' UNION ALL
    SELECT 'MOCK_SELLER_A', '2017-03-01'::DATE, FALSE, 'at_risk' UNION ALL
    SELECT 'MOCK_SELLER_A', '2017-04-01'::DATE, FALSE, 'at_risk' UNION ALL
    SELECT 'MOCK_SELLER_A', '2017-05-01'::DATE, FALSE, 'churned' UNION ALL
    SELECT 'MOCK_SELLER_A', '2017-06-01'::DATE, FALSE, 'dormant' UNION ALL
    SELECT 'MOCK_SELLER_A', '2017-07-01'::DATE, TRUE, 'reactivated' UNION ALL

    -- Mock Seller B (6-month Churn & Reactivation Sequence):
    -- Month 1 (First sale) -> 'new'
    -- Month 2 (No sale, 1st inactive month) -> 'at_risk'
    -- Month 3 (No sale, 2nd inactive month) -> 'at_risk'
    -- Month 4 (No sale, 3rd inactive month) -> 'churned'
    -- Month 5 (No sale, 4th inactive month) -> 'dormant'
    -- Month 6 (Sale resumes) -> 'reactivated'
    SELECT 'MOCK_SELLER_B', '2017-01-01'::DATE, TRUE, 'new' UNION ALL
    SELECT 'MOCK_SELLER_B', '2017-02-01'::DATE, FALSE, 'at_risk' UNION ALL
    SELECT 'MOCK_SELLER_B', '2017-03-01'::DATE, FALSE, 'at_risk' UNION ALL
    SELECT 'MOCK_SELLER_B', '2017-04-01'::DATE, FALSE, 'churned' UNION ALL
    SELECT 'MOCK_SELLER_B', '2017-05-01'::DATE, FALSE, 'dormant' UNION ALL
    SELECT 'MOCK_SELLER_B', '2017-06-01'::DATE, TRUE, 'reactivated' UNION ALL

    -- Mock Seller C (Quick Recovery: Sale after 2 inactive months remains 'active', not 'reactivated'):
    SELECT 'MOCK_SELLER_C', '2017-01-01'::DATE, TRUE, 'new' UNION ALL
    SELECT 'MOCK_SELLER_C', '2017-02-01'::DATE, FALSE, 'at_risk' UNION ALL
    SELECT 'MOCK_SELLER_C', '2017-03-01'::DATE, FALSE, 'at_risk' UNION ALL
    SELECT 'MOCK_SELLER_C', '2017-04-01'::DATE, TRUE, 'active'

),

lifecycle_tracking AS (

    SELECT
        seller_id,
        month,
        is_active,
        expected_lifecycle_status,
        ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY month) AS month_seq,
        MAX(CASE WHEN is_active THEN month END) OVER (
            PARTITION BY seller_id ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS last_active_month
    FROM mock_seller_history

),

inactivity_spells AS (

    SELECT
        *,
        CASE 
            WHEN is_active THEN 0
            WHEN last_active_month IS NULL THEN month_seq
            ELSE DATEDIFF('month', last_active_month, month)
        END AS consecutive_inactive_months,
        LAG(CASE 
            WHEN is_active THEN 0
            WHEN last_active_month IS NULL THEN month_seq
            ELSE DATEDIFF('month', last_active_month, month)
        END) OVER (PARTITION BY seller_id ORDER BY month) AS prior_inactive_months
    FROM lifecycle_tracking

),

lifecycle_assigned AS (

    SELECT
        seller_id,
        month,
        is_active,
        consecutive_inactive_months,
        prior_inactive_months,
        expected_lifecycle_status,
        -- Exact CASE logic from fct_seller_monthly_performance
        CASE
            WHEN month_seq = 1 THEN 'new'
            WHEN is_active AND COALESCE(prior_inactive_months, 0) >= 3 THEN 'reactivated'
            WHEN is_active THEN 'active'
            WHEN consecutive_inactive_months IN (1, 2) THEN 'at_risk'
            WHEN consecutive_inactive_months = 3 THEN 'churned'
            ELSE 'dormant'
        END AS calculated_lifecycle_status
    FROM inactivity_spells

)

-- Filter for rows where calculated != expected (returns 0 rows on pass)
SELECT
    seller_id,
    month,
    is_active,
    consecutive_inactive_months,
    prior_inactive_months,
    expected_lifecycle_status,
    calculated_lifecycle_status
FROM lifecycle_assigned
WHERE calculated_lifecycle_status != expected_lifecycle_status
