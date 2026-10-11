{{
    config(
        materialized='incremental',
        unique_key='order_item_key',
        incremental_strategy='merge'
    )
}}

WITH items_joined AS (

    SELECT *
    FROM {{ ref('int_order_items_sellers_joined') }}
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
        MD5(items_joined.order_id || '-' || items_joined.order_item_id::VARCHAR) AS order_item_key,

        -- Degenerate Dimensions & Natural Keys
        items_joined.order_id,
        items_joined.order_item_id,

        -- Dimension Foreign Keys
        MD5(items_joined.order_purchase_timestamp::DATE::VARCHAR) AS purchase_date_key,
        MD5(items_joined.seller_id) AS seller_key,
        MD5(items_joined.product_id) AS product_key,
        MD5(customers.customer_unique_id) AS customer_key,

        -- Natural Foreign Identifiers
        items_joined.seller_id,
        items_joined.product_id,
        customers.customer_unique_id,

        -- Lifecycle Timestamp for Incremental Partitions
        items_joined.order_purchase_timestamp,
        items_joined.shipping_limit_date,

        -- Additive Financial Measures
        items_joined.price,
        items_joined.freight_value,
        items_joined.total_item_value,

        -- Audit Metadata
        CURRENT_TIMESTAMP() AS _loaded_at

    FROM items_joined
    LEFT JOIN customers
        ON items_joined.customer_id = customers.customer_id

)

SELECT * FROM final
