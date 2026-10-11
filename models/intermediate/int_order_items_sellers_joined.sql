WITH items AS (

    SELECT *
    FROM {{ ref('stg_order_items') }}

),

orders AS (

    SELECT *
    FROM {{ ref('stg_orders') }}

),

sellers AS (

    SELECT *
    FROM {{ ref('stg_sellers') }}

),

products AS (

    SELECT *
    FROM {{ ref('stg_products') }}

),

translations AS (

    SELECT *
    FROM {{ ref('stg_product_translations') }}

),

joined AS (

    SELECT
        -- Primary Key
        items.order_id,
        items.order_item_id,

        -- Foreign Keys
        items.product_id,
        items.seller_id,
        orders.customer_id,

        -- Order Lifecycle & Status
        orders.order_status,
        orders.order_purchase_timestamp,
        orders.order_approved_at,
        orders.order_delivered_carrier_date,
        orders.order_delivered_customer_date,
        orders.order_estimated_delivery_date,

        -- Transaction Metrics
        items.shipping_limit_date,
        items.price,
        items.freight_value,
        (items.price + items.freight_value) AS total_item_value,

        -- Seller Geography
        sellers.seller_zip_code_prefix,
        sellers.seller_city,
        sellers.seller_state,

        -- Product Attributes
        products.product_category_name,
        COALESCE(
            translations.product_category_name_english, products.product_category_name, 'unknown'
        ) AS product_category_name_english,
        products.product_name_lenght,
        products.product_description_lenght,
        products.product_photos_qty,
        products.product_weight_g,
        products.product_length_cm,
        products.product_height_cm,
        products.product_width_cm,

        -- Technical Audit Columns
        items._batch_id,
        items._as_of_date,
        items._extracted_at,
        items._loaded_at

    FROM items
    INNER JOIN orders
        ON items.order_id = orders.order_id
    INNER JOIN sellers
        ON items.seller_id = sellers.seller_id
    INNER JOIN products
        ON items.product_id = products.product_id
    LEFT JOIN translations
        ON products.product_category_name = translations.product_category_name

)

SELECT * FROM joined
