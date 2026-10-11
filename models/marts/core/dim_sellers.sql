WITH sellers AS (

    SELECT *
    FROM {{ ref('stg_sellers') }}

),

seller_orders AS (

    SELECT *
    FROM {{ ref('int_seller_orders_aggregated') }}
    WHERE NOT is_canceled AND NOT is_unavailable

),

seller_metrics AS (

    SELECT
        seller_id,
        MIN(order_purchase_timestamp)::DATE AS first_sale_date,
        MAX(order_purchase_timestamp)::DATE AS last_sale_date,
        COUNT(DISTINCT DATE_TRUNC('month', order_purchase_timestamp)) AS active_months_count
    FROM seller_orders
    GROUP BY seller_id

)

SELECT
    MD5(sellers.seller_id) AS seller_key,
    sellers.seller_id,
    sellers.seller_zip_code_prefix,
    sellers.seller_city,
    sellers.seller_state,
    m.first_sale_date,
    m.last_sale_date,
    COALESCE(m.active_months_count, 0) AS active_months_count
FROM sellers
LEFT JOIN seller_metrics m
    ON sellers.seller_id = m.seller_id
