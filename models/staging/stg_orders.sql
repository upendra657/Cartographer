WITH source AS (

    SELECT *
    FROM cartographer_db.raw.raw_orders

),

deduplicated AS (

    SELECT
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        _batch_id,
        _as_of_date,
        _extracted_at,
        _loaded_at
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY _extracted_at DESC
    ) = 1

)

SELECT * FROM deduplicated
