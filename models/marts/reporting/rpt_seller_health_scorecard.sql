WITH monthly_performance AS (

    SELECT *
    FROM {{ ref('fct_seller_monthly_performance') }}

),

sellers AS (

    SELECT *
    FROM {{ ref('dim_sellers') }}

),

scorecard AS (

    SELECT
        -- Surrogate Key & Identifiers
        perf.seller_monthly_key,
        perf.seller_id,
        perf.month,
        perf.date_key,
        perf.seller_key,

        -- Seller Dimension Attributes
        sellers.seller_city,
        sellers.seller_state,
        sellers.seller_zip_code_prefix,
        sellers.first_sale_date,
        sellers.last_sale_date,
        DATEDIFF('month', sellers.first_sale_date, perf.month) AS tenure_months_at_reporting,

        -- Core Operational & Financial Measures
        perf.seller_orders_count,
        perf.items_count,
        perf.gmv,
        perf.freight,

        -- Quality & Fulfillment SLA Metrics
        perf.sla_hits,
        perf.sla_eligible,
        perf.fulfillment_sla_rate,
        perf.defects_count,
        perf.defect_eligible,
        perf.order_defect_rate,

        -- Lifecycle & Account Health Tiers
        perf.is_active_month,
        perf.consecutive_inactive_months,
        perf.seller_lifecycle_status,
        perf.t3m_seller_orders,
        perf.health_tier,

        -- Maturity Flag
        perf.is_final

    FROM monthly_performance perf
    INNER JOIN sellers
        ON perf.seller_id = sellers.seller_id

)

SELECT * FROM scorecard
