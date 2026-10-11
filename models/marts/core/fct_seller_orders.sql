{{
    config(
        materialized='incremental',
        unique_key='seller_order_key',
        incremental_strategy='merge'
    )
}}

WITH seller_orders AS (

    SELECT *
    FROM {{ ref('int_seller_orders_aggregated') }}
    {% if is_incremental() %}
        WHERE order_purchase_timestamp >= (
            SELECT DATEADD('month', -{{ var('lookback_months', 2) }}, MAX(order_purchase_timestamp))
            FROM {{ this }}
        )
    {% endif %}

),

customers AS (

    SELECT
        customer_id,
        customer_unique_id
    FROM {{ ref('stg_customers') }}

),

final AS (

    SELECT
        -- Surrogate Primary Key
        MD5(seller_orders.seller_id || '-' || seller_orders.order_id) AS seller_order_key,

        -- Natural Keys & Degenerate Dimension
        seller_orders.seller_id,
        seller_orders.order_id,
        customers.customer_unique_id,

        -- Dimension Foreign Keys
        MD5(seller_orders.seller_id) AS seller_key,
        MD5(customers.customer_unique_id) AS customer_key,
        MD5(seller_orders.order_purchase_timestamp::DATE::VARCHAR) AS purchase_date_key,
        MD5(seller_orders.shipping_limit_date::DATE::VARCHAR) AS shipping_limit_date_key,
        IFF(
            seller_orders.order_delivered_carrier_date IS NOT NULL,
            MD5(seller_orders.order_delivered_carrier_date::DATE::VARCHAR),
            NULL
        ) AS carrier_handoff_date_key,
        IFF(
            seller_orders.order_delivered_customer_date IS NOT NULL,
            MD5(seller_orders.order_delivered_customer_date::DATE::VARCHAR),
            NULL
        ) AS customer_delivery_date_key,
        MD5(seller_orders.order_estimated_delivery_date::DATE::VARCHAR)
            AS estimated_delivery_date_key,

        -- Accumulating Snapshot Milestones
        seller_orders.order_status,
        seller_orders.order_purchase_timestamp,
        seller_orders.order_approved_at,
        seller_orders.shipping_limit_date,
        seller_orders.order_delivered_carrier_date,
        seller_orders.order_delivered_customer_date,
        seller_orders.order_estimated_delivery_date,

        -- Lifecycle Duration Lags
        DATEDIFF('hour', seller_orders.order_purchase_timestamp, seller_orders.order_approved_at)
            AS hours_to_approval,
        DATEDIFF(
            'hour',
            seller_orders.order_purchase_timestamp,
            seller_orders.order_delivered_carrier_date
        ) AS hours_to_carrier_handoff,
        DATEDIFF(
            'day',
            seller_orders.order_purchase_timestamp,
            seller_orders.order_delivered_customer_date
        ) AS days_to_customer_delivery,
        DATEDIFF(
            'day',
            seller_orders.order_delivered_carrier_date,
            seller_orders.order_delivered_customer_date
        ) AS carrier_transit_days,
        DATEDIFF(
            'day',
            seller_orders.order_estimated_delivery_date,
            seller_orders.order_delivered_customer_date
        ) AS delivery_delta_vs_estimate_days,

        -- Performance & Operational Flags
        seller_orders.is_canceled,
        seller_orders.is_unavailable,
        seller_orders.is_multi_seller_order,
        seller_orders.is_impossible_sequence,
        seller_orders.is_handoff_on_time,
        seller_orders.is_overdue_unshipped,
        seller_orders.is_sla_eligible,
        seller_orders.is_sla_hit,
        seller_orders.is_delivered_late,
        seller_orders.is_order_defect,
        seller_orders.is_stuck_in_transit,

        -- Measures & Review Scores
        seller_orders.item_count,
        seller_orders.seller_order_gmv,
        seller_orders.seller_order_freight,
        seller_orders.seller_order_total_value,
        seller_orders.review_count,
        seller_orders.worst_review_score,
        seller_orders.latest_review_score,

        -- Audit Metadata
        CURRENT_TIMESTAMP() AS _loaded_at

    FROM seller_orders
    LEFT JOIN customers
        ON seller_orders.customer_id = customers.customer_id

)

SELECT * FROM final
