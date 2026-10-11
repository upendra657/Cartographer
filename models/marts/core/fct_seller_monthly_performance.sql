{{
    config(
        materialized='table'
    )
}}

WITH spine AS (

    SELECT *
    FROM {{ ref('int_seller_month_spine') }}

),

seller_orders AS (

    SELECT
        seller_id,
        DATE_TRUNC('month', order_purchase_timestamp)::DATE AS month,
        order_id,
        order_status,
        item_count,
        seller_order_gmv,
        seller_order_freight,
        is_sla_hit,
        is_sla_eligible,
        is_order_defect,
        is_delivered_late,
        is_canceled,
        is_unavailable
    FROM {{ ref('int_seller_orders_aggregated') }}

),

monthly_aggregates AS (

    SELECT
        spine.seller_id,
        spine.month,

        -- Order & Item Volumes
        COUNT(so.order_id) AS seller_orders_count,
        COALESCE(SUM(so.item_count), 0) AS items_count,

        -- Financial Metrics (Excluding canceled/unavailable per D1)
        COALESCE(
            SUM(
                CASE
                    WHEN
                        NOT so.is_canceled AND NOT so.is_unavailable
                        THEN so.seller_order_gmv
                    ELSE 0
                END
            ),
            0
        ) AS gmv,
        COALESCE(
            SUM(
                CASE
                    WHEN
                        NOT so.is_canceled AND NOT so.is_unavailable
                        THEN so.seller_order_freight
                    ELSE 0
                END
            ),
            0
        ) AS freight,

        -- Fulfillment SLA Numerator & Denominator
        COUNT_IF(so.is_sla_hit) AS sla_hits,
        COUNT_IF(so.is_sla_eligible) AS sla_eligible,

        -- Order Defect Rate (ODR) Numerator & Denominator
        COUNT_IF(so.is_order_defect) AS defects_count,
        COUNT(so.order_id) AS defect_eligible,

        -- Delivery Promise Lateness
        COUNT_IF(so.is_delivered_late) AS delivered_late_count,
        COUNT_IF(so.order_status = 'delivered') AS delivered_count,

        -- Cancellations
        COUNT_IF(so.is_canceled) AS canceled_count,

        -- Active Month Indicator (at least one non-canceled/non-unavailable order)
        (COUNT_IF(NOT so.is_canceled AND NOT so.is_unavailable) > 0) AS is_active_month

    FROM spine
    LEFT JOIN seller_orders so
        ON
            spine.seller_id = so.seller_id
            AND spine.month = so.month
    GROUP BY spine.seller_id, spine.month

),

lifecycle_tracking AS (

    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY month) AS month_seq,
        -- Running count of active months
        COUNT_IF(is_active_month)
            OVER (PARTITION BY seller_id ORDER BY month ROWS UNBOUNDED PRECEDING)
            AS cumulative_active_months,
        -- Last active month seen
        MAX(CASE WHEN is_active_month THEN month END) OVER (
            PARTITION BY seller_id ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS last_active_month
    FROM monthly_aggregates

),

inactivity_spells AS (

    SELECT
        *,
        CASE
            WHEN is_active_month THEN 0
            WHEN last_active_month IS NULL THEN month_seq
            ELSE DATEDIFF('month', last_active_month, month)
        END AS consecutive_inactive_months,
        -- Inactive spell just prior to current month
        LAG(CASE
            WHEN is_active_month THEN 0
            WHEN last_active_month IS NULL THEN month_seq
            ELSE DATEDIFF('month', last_active_month, month)
        END) OVER (PARTITION BY seller_id ORDER BY month) AS prior_inactive_months
    FROM lifecycle_tracking

),

lifecycle_assigned AS (

    SELECT
        *,
        CASE
            WHEN month_seq = 1 THEN 'new'
            WHEN is_active_month AND COALESCE(prior_inactive_months, 0) >= 3 THEN 'reactivated'
            WHEN is_active_month THEN 'active'
            WHEN consecutive_inactive_months IN (1, 2) THEN 'at_risk'
            WHEN consecutive_inactive_months = 3 THEN 'churned'
            ELSE 'dormant'
        END AS seller_lifecycle_status
    FROM inactivity_spells

),

trailing_metrics AS (

    SELECT
        *,
        -- Trailing 3-month volume window for health classification (D7)
        SUM(seller_orders_count) OVER (
            PARTITION BY seller_id ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS t3m_seller_orders,
        SUM(sla_hits) OVER (
            PARTITION BY seller_id ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS t3m_sla_hits,
        SUM(sla_eligible) OVER (
            PARTITION BY seller_id ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS t3m_sla_eligible,
        SUM(defects_count) OVER (
            PARTITION BY seller_id ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS t3m_defects,
        SUM(defect_eligible) OVER (
            PARTITION BY seller_id ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS t3m_defect_eligible
    FROM lifecycle_assigned

),

final AS (

    SELECT
        -- Primary Key
        MD5(seller_id || '-' || month::VARCHAR) AS seller_monthly_key,

        -- Natural Keys
        seller_id,
        month,
        MD5(seller_id) AS seller_key,
        MD5(month::VARCHAR) AS date_key,

        -- Monthly Quantities & Revenue
        seller_orders_count,
        items_count,
        gmv,
        freight,

        -- SLA Numerator / Denominator
        sla_hits,
        sla_eligible,
        ROUND(sla_hits / NULLIF(sla_eligible, 0), 4) AS fulfillment_sla_rate,

        -- ODR Numerator / Denominator
        defects_count,
        defect_eligible,
        ROUND(defects_count / NULLIF(defect_eligible, 0), 4) AS order_defect_rate,

        -- Delivery & Quality
        delivered_late_count,
        delivered_count,
        canceled_count,

        -- Lifecycle Status (§4.4)
        is_active_month,
        consecutive_inactive_months,
        seller_lifecycle_status,

        -- Health Tier Assignment (D7)
        t3m_seller_orders,
        CASE
            WHEN t3m_seller_orders < 10 THEN 'insufficient_volume'
            WHEN
                (t3m_sla_hits / NULLIF(t3m_sla_eligible, 0)) >= 0.90
                AND (t3m_defects / NULLIF(t3m_defect_eligible, 0)) <= 0.10 THEN 'Green'
            WHEN
                (t3m_sla_hits / NULLIF(t3m_sla_eligible, 0)) >= 0.80
                AND (t3m_defects / NULLIF(t3m_defect_eligible, 0)) <= 0.15 THEN 'Amber'
            ELSE 'Red'
        END AS health_tier,

        -- Maturity Flag (§4.5: closed once 2 full months have passed)
        (month <= '2018-06-01'::DATE) AS is_final,

        -- Audit Metadata
        CURRENT_TIMESTAMP() AS _loaded_at

    FROM trailing_metrics

)

SELECT * FROM final
