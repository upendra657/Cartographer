-- Unit Test: Health Tier Gating & Fulfillment SLA Edge Cases (Step 3.10)
-- Validates:
-- 1. Volume-gated executive health tiers (D7):
--    - Volume < 10 orders -> 'insufficient_volume' (even with 100% SLA or 80% ODR)
--    - Volume >= 10 orders -> 'Green' (SLA >= 90% and ODR <= 10%)
--    - Volume >= 10 orders -> 'Amber' (SLA >= 80% and ODR <= 15%)
--    - Volume >= 10 orders -> 'Red' (otherwise)
-- 2. Fulfillment SLA edge cases (D2, D3):
--    - Overdue unshipped order: order_delivered_carrier_date IS NULL and cutoff > shipping_limit_date -> is_sla_eligible = TRUE, is_sla_hit = FALSE (miss)
--    - On-time carrier handoff: carrier_date <= shipping_limit_date -> is_sla_eligible = TRUE, is_sla_hit = TRUE
--    - Late carrier handoff: carrier_date > shipping_limit_date -> is_sla_eligible = TRUE, is_sla_hit = FALSE
--    - Impossible sequence (carrier < purchase): excluded -> is_sla_eligible = FALSE, is_sla_hit = FALSE
--    - Canceled order: excluded -> is_sla_eligible = FALSE, is_sla_hit = FALSE
-- Returns 0 rows on pass.

WITH mock_health_tiers AS (

    -- Low volume (< 10) with perfect metrics -> must be 'insufficient_volume', NOT 'Green'
    SELECT 'CASE_1_LOW_VOL_PERFECT' AS test_case, 9 AS t3m_orders, 9 AS t3m_sla_hits, 9 AS t3m_sla_eligible, 0 AS t3m_defects, 9 AS t3m_defect_eligible, 'insufficient_volume' AS expected_tier UNION ALL
    -- Sufficient volume (>= 10) with excellent metrics -> 'Green'
    SELECT 'CASE_2_SUFFICIENT_GREEN', 10, 10, 10, 0, 10, 'Green' UNION ALL
    -- Sufficient volume with acceptable metrics -> 'Amber'
    SELECT 'CASE_3_SUFFICIENT_AMBER', 15, 13, 15, 2, 15, 'Amber' UNION ALL
    -- Sufficient volume with substandard metrics -> 'Red'
    SELECT 'CASE_4_SUFFICIENT_RED', 20, 14, 20, 4, 20, 'Red' UNION ALL
    -- Low volume (< 10) with terrible metrics -> must be 'insufficient_volume', NOT 'Red'
    SELECT 'CASE_5_LOW_VOL_DEFECTIVE', 5, 2, 5, 4, 5, 'insufficient_volume'

),

evaluated_health_tiers AS (

    SELECT
        test_case,
        expected_tier,
        -- Exact health tier logic from fct_seller_monthly_performance
        CASE
            WHEN t3m_orders < 10 THEN 'insufficient_volume'
            WHEN (t3m_sla_hits / NULLIF(t3m_sla_eligible, 0)) >= 0.90 
                 AND (t3m_defects / NULLIF(t3m_defect_eligible, 0)) <= 0.10 THEN 'Green'
            WHEN (t3m_sla_hits / NULLIF(t3m_sla_eligible, 0)) >= 0.80 
                 AND (t3m_defects / NULLIF(t3m_defect_eligible, 0)) <= 0.15 THEN 'Amber'
            ELSE 'Red'
        END AS calculated_tier
    FROM mock_health_tiers

),

health_tier_discrepancies AS (

    SELECT
        'HEALTH_TIER_FAIL: ' || test_case AS discrepancy_description
    FROM evaluated_health_tiers
    WHERE calculated_tier != expected_tier

),

mock_sla_orders AS (

    -- 1. Overdue & unshipped (carrier is NULL, deadline passed -> SLA miss: eligible = TRUE, hit = FALSE) (D2)
    SELECT
        'CASE_1_OVERDUE_UNSHIPPED' AS test_case,
        'processing' AS order_status,
        '2018-05-01 10:00:00'::TIMESTAMP_NTZ AS purchase_date,
        '2018-05-05 18:00:00'::TIMESTAMP_NTZ AS shipping_limit,
        NULL::TIMESTAMP_NTZ AS carrier_date,
        '2018-05-10 00:00:00'::TIMESTAMP_NTZ AS evaluation_time,
        TRUE AS exp_sla_eligible,
        FALSE AS exp_sla_hit
    UNION ALL

    -- 2. On-time handoff (carrier <= deadline -> eligible = TRUE, hit = TRUE)
    SELECT
        'CASE_2_ON_TIME_HANDOFF',
        'delivered',
        '2018-05-01 10:00:00'::TIMESTAMP_NTZ,
        '2018-05-05 18:00:00'::TIMESTAMP_NTZ,
        '2018-05-04 12:00:00'::TIMESTAMP_NTZ,
        '2018-05-10 00:00:00'::TIMESTAMP_NTZ,
        TRUE,
        TRUE
    UNION ALL

    -- 3. Late handoff (carrier > deadline -> eligible = TRUE, hit = FALSE)
    SELECT
        'CASE_3_LATE_HANDOFF',
        'delivered',
        '2018-05-01 10:00:00'::TIMESTAMP_NTZ,
        '2018-05-05 18:00:00'::TIMESTAMP_NTZ,
        '2018-05-06 12:00:00'::TIMESTAMP_NTZ,
        '2018-05-10 00:00:00'::TIMESTAMP_NTZ,
        TRUE,
        FALSE
    UNION ALL

    -- 4. Impossible sequence (carrier < purchase -> excluded: eligible = FALSE, hit = FALSE) (F9)
    SELECT
        'CASE_4_IMPOSSIBLE_SEQUENCE',
        'delivered',
        '2018-05-05 10:00:00'::TIMESTAMP_NTZ,
        '2018-05-10 18:00:00'::TIMESTAMP_NTZ,
        '2018-05-04 10:00:00'::TIMESTAMP_NTZ,
        '2018-05-10 00:00:00'::TIMESTAMP_NTZ,
        FALSE,
        FALSE
    UNION ALL

    -- 5. Canceled order (excluded -> eligible = FALSE, hit = FALSE)
    SELECT
        'CASE_5_CANCELED_ORDER',
        'canceled',
        '2018-05-01 10:00:00'::TIMESTAMP_NTZ,
        '2018-05-05 18:00:00'::TIMESTAMP_NTZ,
        NULL::TIMESTAMP_NTZ,
        '2018-05-10 00:00:00'::TIMESTAMP_NTZ,
        FALSE,
        FALSE

),

evaluated_sla_orders AS (

    SELECT
        test_case,
        exp_sla_eligible,
        exp_sla_hit,
        -- Exact SLA eligibility logic from int_seller_orders_aggregated
        (
            order_status NOT IN ('canceled', 'unavailable')
            AND NOT (
                carrier_date IS NOT NULL
                AND carrier_date < purchase_date
            )
            AND (
                carrier_date IS NOT NULL
                OR evaluation_time > shipping_limit
            )
        ) AS calc_sla_eligible,
        -- Exact SLA hit logic from int_seller_orders_aggregated
        (
            order_status NOT IN ('canceled', 'unavailable')
            AND NOT (
                carrier_date IS NOT NULL
                AND carrier_date < purchase_date
            )
            AND carrier_date IS NOT NULL
            AND carrier_date <= shipping_limit
            AND carrier_date >= purchase_date
        ) AS calc_sla_hit
    FROM mock_sla_orders

),

sla_discrepancies AS (

    SELECT
        'SLA_LOGIC_FAIL: ' || test_case AS discrepancy_description
    FROM evaluated_sla_orders
    WHERE calc_sla_eligible != exp_sla_eligible
       OR calc_sla_hit != exp_sla_hit

),

unioned_discrepancies AS (

    SELECT discrepancy_description FROM health_tier_discrepancies
    UNION ALL
    SELECT discrepancy_description FROM sla_discrepancies

)

-- Filter for discrepancies (returns 0 rows on pass)
SELECT * FROM unioned_discrepancies
