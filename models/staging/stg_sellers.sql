WITH source AS (

    SELECT *
    FROM cartographer_db.raw.raw_sellers

),

deduplicated AS (

    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY seller_id
        ORDER BY _extracted_at DESC
    ) = 1

)

SELECT * FROM deduplicated
