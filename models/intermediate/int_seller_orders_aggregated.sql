WITH items AS (

    SELECT
        order_id,
        seller_id,
        COUNT(order_item_id) AS item_count,
        SUM(price) AS seller_order_gmv,
        SUM(freight_value) AS seller_order_freight,
        MAX(shipping_limit_date) AS shipping_limit_date
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id, seller_id

),

orders AS (

    SELECT *
    FROM {{ ref('stg_orders') }}

),

reviews AS (

    SELECT *
    FROM {{ ref('int_reviews_rolled_up_to_orders') }}

),

order_seller_counts AS (

    SELECT
        order_id,
        COUNT(DISTINCT seller_id) AS seller_count_on_order
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id

),

joined AS (

    SELECT
        -- Grain: seller_id, order_id
        items.seller_id,
        items.order_id,

        -- Customer Foreign Key
        orders.customer_id,

        -- Order Lifecycle Timestamps
        orders.order_status,
        orders.order_purchase_timestamp,
        orders.order_approved_at,
        items.shipping_limit_date,
        orders.order_delivered_carrier_date,
        orders.order_delivered_customer_date,
        orders.order_estimated_delivery_date,

        -- Review Metrics
        reviews.review_count,
        reviews.worst_review_score,
        reviews.latest_review_score,

        -- Aggregated Financials & Quantity
        items.item_count,
        items.seller_order_gmv,
        items.seller_order_freight,
        (items.seller_order_gmv + items.seller_order_freight) AS seller_order_total_value,

        -- Multi-Seller Flag
        (order_seller_counts.seller_count_on_order > 1) AS is_multi_seller_order,

        -- Anomaly Flag: Handoff before purchase (F9: 166 orders)
        (
            orders.order_delivered_carrier_date IS NOT NULL
            AND orders.order_delivered_carrier_date < orders.order_purchase_timestamp
        ) AS is_impossible_sequence,

        -- On-Time Handoff Flag
        (
            orders.order_delivered_carrier_date IS NOT NULL
            AND orders.order_delivered_carrier_date <= items.shipping_limit_date
            AND (orders.order_delivered_carrier_date >= orders.order_purchase_timestamp)
        ) AS is_handoff_on_time,

        -- Overdue and Unshipped Flag (D2: 599 seller-orders counted as SLA misses)
        (
            orders.order_delivered_carrier_date IS NULL
            AND orders.order_status NOT IN ('canceled', 'unavailable')
            AND items.shipping_limit_date < '2018-09-01'::TIMESTAMP_NTZ
        ) AS is_overdue_unshipped,

        -- SLA Eligibility (not canceled/unavailable, not impossible sequence,
        -- handed off or overdue)
        (
            orders.order_status NOT IN ('canceled', 'unavailable')
            AND NOT (
                orders.order_delivered_carrier_date IS NOT NULL
                AND orders.order_delivered_carrier_date < orders.order_purchase_timestamp
            )
            AND (
                orders.order_delivered_carrier_date IS NOT NULL
                OR items.shipping_limit_date < '2018-09-01'::TIMESTAMP_NTZ
            )
        ) AS is_sla_eligible,

        -- SLA Hit (SLA eligible and handed off on time)
        (
            orders.order_status NOT IN ('canceled', 'unavailable')
            AND NOT (
                orders.order_delivered_carrier_date IS NOT NULL
                AND orders.order_delivered_carrier_date < orders.order_purchase_timestamp
            )
            AND orders.order_delivered_carrier_date IS NOT NULL
            AND orders.order_delivered_carrier_date <= items.shipping_limit_date
            AND orders.order_delivered_carrier_date >= orders.order_purchase_timestamp
        ) AS is_sla_hit,

        -- Customer Delivery Punctuality (Date Grain per F10)
        (
            orders.order_delivered_customer_date IS NOT NULL
            AND orders.order_delivered_customer_date::DATE
            > orders.order_estimated_delivery_date::DATE
        ) AS is_delivered_late,

        -- Order Defect (worst review score <= 2)
        (
            COALESCE(reviews.worst_review_score <= 2, FALSE)
        ) AS is_order_defect,

        -- Canceled Order Flag
        (orders.order_status = 'canceled') AS is_canceled,

        -- Unavailable Order Flag
        (orders.order_status = 'unavailable') AS is_unavailable,

        -- Stuck in Transit Flag (F11: shipped > 60 days without delivery)
        (
            orders.order_delivered_carrier_date IS NOT NULL
            AND orders.order_delivered_customer_date IS NULL
            AND DATEDIFF('day', orders.order_delivered_carrier_date, '2018-09-01'::DATE) > 60
        ) AS is_stuck_in_transit,

        -- Technical Audit Metadata
        orders._loaded_at

    FROM items
    INNER JOIN orders
        ON items.order_id = orders.order_id
    INNER JOIN order_seller_counts
        ON items.order_id = order_seller_counts.order_id
    LEFT JOIN reviews
        ON items.order_id = reviews.order_id

)

SELECT * FROM joined
