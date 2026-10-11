WITH customers AS (

    SELECT *
    FROM {{ ref('stg_customers') }}

),

orders AS (

    SELECT *
    FROM {{ ref('stg_orders') }}

),

customer_orders AS (

    SELECT
        c.customer_id,
        c.customer_unique_id,
        c.customer_zip_code_prefix,
        c.customer_city,
        c.customer_state,
        o.order_id,
        o.order_purchase_timestamp
    FROM customers c
    LEFT JOIN orders o
        ON c.customer_id = o.customer_id

),

customer_metrics AS (

    SELECT
        customer_unique_id,
        COUNT(DISTINCT order_id) AS lifetime_order_count,
        MIN(order_purchase_timestamp)::DATE AS first_order_date,
        MAX(order_purchase_timestamp)::DATE AS last_order_date,
        (COUNT(DISTINCT order_id) > 1) AS is_repeat_buyer
    FROM customer_orders
    GROUP BY customer_unique_id

),

latest_customer_location AS (

    SELECT
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    FROM customer_orders
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_unique_id
        ORDER BY order_purchase_timestamp DESC NULLS LAST, customer_id DESC
    ) = 1

)

SELECT
    MD5(m.customer_unique_id) AS customer_key,
    m.customer_unique_id,
    loc.customer_zip_code_prefix,
    loc.customer_city,
    loc.customer_state,
    m.first_order_date,
    m.last_order_date,
    m.lifetime_order_count,
    m.is_repeat_buyer
FROM customer_metrics m
INNER JOIN latest_customer_location loc
    ON m.customer_unique_id = loc.customer_unique_id
