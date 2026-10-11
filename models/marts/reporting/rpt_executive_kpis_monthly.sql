WITH monthly_seller_performance AS (

    SELECT *
    FROM {{ ref('fct_seller_monthly_performance') }}

),

monthly_aggregates AS (

    SELECT
        month,
        MD5(month::VARCHAR) AS date_key,

        -- Platform Financial Totals
        SUM(gmv) AS total_gmv,
        SUM(freight) AS total_freight,
        SUM(seller_orders_count) AS total_seller_orders,
        SUM(items_count) AS total_items_sold,

        -- Partner Pool & Lifecycle Counts
        COUNT(DISTINCT seller_id) AS total_monitored_sellers,
        COUNT_IF(is_active_month) AS active_sellers_count,
        COUNT_IF(seller_lifecycle_status = 'new') AS new_sellers_count,
        COUNT_IF(seller_lifecycle_status = 'at_risk') AS at_risk_sellers_count,
        COUNT_IF(seller_lifecycle_status = 'churned') AS churned_sellers_count,
        COUNT_IF(seller_lifecycle_status = 'dormant') AS dormant_sellers_count,
        COUNT_IF(seller_lifecycle_status = 'reactivated') AS reactivated_sellers_count,

        -- Fulfillment SLA & Quality Performance (Macro-Level)
        SUM(sla_hits) AS total_sla_hits,
        SUM(sla_eligible) AS total_sla_eligible,
        ROUND(SUM(sla_hits) / NULLIF(SUM(sla_eligible), 0), 4) AS macro_fulfillment_sla_rate,

        SUM(defects_count) AS total_defects,
        SUM(defect_eligible) AS total_defect_eligible,
        ROUND(SUM(defects_count) / NULLIF(SUM(defect_eligible), 0), 4) AS macro_order_defect_rate,

        -- Executive Health Tier Breakdown
        COUNT_IF(health_tier = 'Green') AS green_sellers_count,
        COUNT_IF(health_tier = 'Amber') AS amber_sellers_count,
        COUNT_IF(health_tier = 'Red') AS red_sellers_count,
        COUNT_IF(health_tier = 'insufficient_volume') AS low_volume_sellers_count,

        -- Period Maturity Flag
        BOOLAND_AGG(is_final) AS is_final

    FROM monthly_seller_performance
    GROUP BY month

),

kpi_calculations AS (

    SELECT
        *,
        -- Active seller pool from M-3 (the only cohort capable of producing
        -- a 3-month churn event in month M per §4.4)
        LAG(active_sellers_count, 3) OVER (ORDER BY month) AS active_sellers_cohort_m_minus_3,

        -- Churn Rate: Churned sellers in month M / Active cohort in M-3
        ROUND(
            churned_sellers_count / NULLIF(LAG(active_sellers_count, 3) OVER (ORDER BY month), 0),
            4
        ) AS monthly_churn_rate

    FROM monthly_aggregates

)

SELECT * FROM kpi_calculations
ORDER BY month
