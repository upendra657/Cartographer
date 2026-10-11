WITH source AS (

    SELECT *
    FROM cartographer_db.raw.raw_product_category_name_translation

),

deduplicated AS (

    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY product_category_name
        ORDER BY _extracted_at DESC
    ) = 1

)

SELECT * FROM deduplicated
