WITH source AS (

    SELECT *
    FROM cartographer_db.raw.raw_order_items

),

deduplicated AS (

    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id, order_item_id
        ORDER BY _extracted_at DESC
    ) = 1

)

SELECT * FROM deduplicated
