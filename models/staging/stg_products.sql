WITH source AS (

    SELECT *
    FROM cartographer_db.raw.raw_products

),

deduplicated AS (

    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY product_id
        ORDER BY _extracted_at DESC
    ) = 1

)

SELECT * FROM deduplicated
