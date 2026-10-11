WITH source AS (

    SELECT *
    FROM cartographer_db.raw.raw_geolocation

),

deduplicated AS (

    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY geolocation_zip_code_prefix
        ORDER BY _extracted_at DESC
    ) = 1

)

SELECT * FROM deduplicated
