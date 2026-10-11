-- Singular Business Logic Unit Test: Seller Lifecycle State Machine Transitions (Step 3.10)
-- Validates:
-- 1. First sale -> 'new'
-- 2. 1st & 2nd consecutive inactive month -> 'at_risk'
-- 3. 3rd consecutive inactive month -> 'churned'
-- 4. 4th+ inactive month -> 'dormant'
-- 5. Sale after 3+ inactive months -> 'reactivated'
-- 6. Sale after 1-2 inactive months -> 'active' (not reactivated)
-- 7. Right-censored sellers (last sale in 2018-07) -> 'at_risk' at 2018-08 (never 'churned')

WITH mocked_seller_history AS (

    -- Seller A: New -> Active -> Inactive 1 (At Risk) -> Inactive 2 (At Risk) -> Inactive 3 (Churned) -> Dormant
    SELECT 'SELLER_A' AS seller_id, '2017-01-01'::DATE AS month, TRUE AS is_active, 'new' AS expected_status UNION ALL
    SELECT 'SELLER_A', '2017-02-01'::DATE, TRUE, 'active' UNION ALL
    SELECT 'SELLER_A', '2017-03-01'::DATE, FALSE, 'at_risk' UNION ALL
    SELECT 'SELLER_A', '2017-04-01'::DATE, FALSE, 'at_risk' UNION ALL
    SELECT 'SELLER_A', '2017-05-01'::DATE, FALSE, 'churned' UNION ALL
    SELECT 'SELLER_A', '2017-06-01'::DATE, FALSE, 'dormant' UNION ALL

    -- Seller B: Reactivation path: New -> Churned -> Reactivated -> Active
    SELECT 'SELLER_B', '2017-01-01'::DATE, TRUE, 'new' UNION ALL
    SELECT 'SELLER_B', '2017-02-01'::DATE, FALSE, 'at_risk' UNION ALL
    SELECT 'SELLER_B', '2017-03-01'::DATE, FALSE, 'at_risk' UNION ALL
    SELECT 'SELLER_B', '2017-04-01'::DATE, FALSE, 'churned' UNION ALL
    SELECT 'SELLER_B', '2017-05-01'::DATE, TRUE, 'reactivated' UNION ALL
    SELECT 'SELLER_B', '2017-06-01'::DATE, TRUE, 'active' UNION ALL

    -- Seller C: Quick recovery: New -> Inactive 1 -> Inactive 2 -> Sale (Active, NOT reactivated)
    SELECT 'SELLER_C', '2017-01-01'::DATE, TRUE, 'new' UNION ALL
    SELECT 'SELLER_C', '2017-02-01'::DATE, FALSE, 'at_risk' UNION ALL
    SELECT 'SELLER_C', '2017-03-01'::DATE, FALSE, 'at_risk' UNION ALL
    SELECT 'SELLER_C', '2017-04-01'::DATE, TRUE, 'active' UNION ALL

    -- Seller D: Right-censoring: Last sale 2018-07, at 2018-08 is at_risk (never churned)
    SELECT 'SELLER_D', '2018-07-01'::DATE, TRUE, 'new' UNION ALL
    SELECT 'SELLER_D', '2018-08-01'::DATE, FALSE, 'at_risk'

),

lifecycle_tracking AS (

    SELECT
        seller_id,
        month,
        is_active,
        expected_status,
        ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY month) AS month_seq,
        MAX(CASE WHEN is_active THEN month END) OVER (
            PARTITION BY seller_id ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS last_active_month
    FROM mocked_seller_history

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

calculated_status AS (

    SELECT
        seller_id,
        month,
        expected_status,
        CASE
            WHEN month_seq = 1 THEN 'new'
            WHEN is_active AND COALESCE(prior_inactive_months, 0) >= 3 THEN 'reactivated'
            WHEN is_active THEN 'active'
            WHEN consecutive_inactive_months IN (1, 2) THEN 'at_risk'
            WHEN consecutive_inactive_months = 3 THEN 'churned'
            ELSE 'dormant'
        END AS actual_status
    FROM inactivity_spells

)

-- Return failures
SELECT *
FROM calculated_status
WHERE actual_status != expected_status
