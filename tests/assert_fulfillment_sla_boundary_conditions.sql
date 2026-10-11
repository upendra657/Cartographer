-- Singular Business Logic Unit Test: SLA Boundary Conditions (Step 3.10)
-- Validates:
-- 1. On time: carrier_date <= shipping_limit -> hit = TRUE, eligible = TRUE
-- 2. Late handoff: carrier_date > shipping_limit -> hit = FALSE, eligible = TRUE
-- 3. Overdue & unshipped: carrier_date IS NULL, deadline passed -> hit = FALSE, eligible = TRUE (D2)
-- 4. Impossible sequence: carrier_date < purchase_date -> excluded (hit = FALSE, eligible = FALSE) (F9)
-- 5. Canceled order: order_status = 'canceled' -> excluded (hit = FALSE, eligible = FALSE)

WITH mocked_orders AS (

    -- 1. On time
    SELECT
        'CASE_1_ON_TIME' AS case_name,
        'delivered' AS order_status,
        '2018-05-01 10:00:00'::TIMESTAMP_NTZ AS purchase_date,
        '2018-05-05 18:00:00'::TIMESTAMP_NTZ AS shipping_limit,
        '2018-05-04 12:00:00'::TIMESTAMP_NTZ AS carrier_date,
        TRUE AS exp_eligible,
        TRUE AS exp_hit
    UNION ALL

    -- 2. Late handoff
    SELECT
        'CASE_2_LATE_HANDOFF',
        'delivered',
        '2018-05-01 10:00:00'::TIMESTAMP_NTZ,
        '2018-05-05 18:00:00'::TIMESTAMP_NTZ,
        '2018-05-06 12:00:00'::TIMESTAMP_NTZ,
        TRUE,
        FALSE
    UNION ALL

    -- 3. Overdue & unshipped (D2)
    SELECT
        'CASE_3_OVERDUE_UNSHIPPED',
        'processing',
        '2018-05-01 10:00:00'::TIMESTAMP_NTZ,
        '2018-05-05 18:00:00'::TIMESTAMP_NTZ,
        NULL,
        TRUE,
        FALSE
    UNION ALL

    -- 4. Impossible sequence (carrier before purchase) (F9)
    SELECT
        'CASE_4_IMPOSSIBLE_SEQUENCE',
        'delivered',
        '2018-05-05 10:00:00'::TIMESTAMP_NTZ,
        '2018-05-10 18:00:00'::TIMESTAMP_NTZ,
        '2018-05-04 10:00:00'::TIMESTAMP_NTZ,
        FALSE,
        FALSE
    UNION ALL

    -- 5. Canceled order
    SELECT
        'CASE_5_CANCELED',
        'canceled',
        '2018-05-01 10:00:00'::TIMESTAMP_NTZ,
        '2018-05-05 18:00:00'::TIMESTAMP_NTZ,
        NULL,
        FALSE,
        FALSE

),

evaluated AS (

    SELECT
        case_name,
        exp_eligible,
        exp_hit,
        -- Evaluated Eligibility
        (
            order_status NOT IN ('canceled', 'unavailable')
            AND NOT (carrier_date IS NOT NULL AND carrier_date < purchase_date)
            AND (carrier_date IS NOT NULL OR shipping_limit < '2018-09-01'::TIMESTAMP_NTZ)
        ) AS act_eligible,
        -- Evaluated Hit
        (
            order_status NOT IN ('canceled', 'unavailable')
            AND NOT (carrier_date IS NOT NULL AND carrier_date < purchase_date)
            AND carrier_date IS NOT NULL
            AND carrier_date <= shipping_limit
            AND carrier_date >= purchase_date
        ) AS act_hit
    FROM mocked_orders

)

-- Return failures
SELECT *
FROM evaluated
WHERE act_eligible != exp_eligible OR act_hit != exp_hit
