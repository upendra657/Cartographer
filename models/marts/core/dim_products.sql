WITH products AS (

    SELECT *
    FROM {{ ref('stg_products') }}

),

translations AS (

    SELECT *
    FROM {{ ref('stg_product_translations') }}

),

overrides AS (

    SELECT *
    FROM {{ ref('category_translation_overrides') }}

)

SELECT
    MD5(p.product_id) AS product_key,
    p.product_id,
    COALESCE(p.product_category_name, 'unknown') AS product_category_name,
    COALESCE(
        o.product_category_name_english,
        t.product_category_name_english,
        p.product_category_name,
        'unknown'
    ) AS product_category_name_english,
    p.product_name_lenght AS product_name_length,
    p.product_description_lenght AS product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
FROM products p
LEFT JOIN translations t
    ON p.product_category_name = t.product_category_name
LEFT JOIN overrides o
    ON p.product_category_name = o.product_category_name
