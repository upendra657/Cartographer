WITH source AS (

    SELECT *
    FROM cartographer_db.raw.raw_order_payments

),

deduplicated AS (

    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id, payment_sequential
        ORDER BY _extracted_at DESC
    ) = 1

)

SELECT * FROM deduplicated
