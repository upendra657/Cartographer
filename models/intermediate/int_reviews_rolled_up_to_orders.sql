WITH reviews AS (

    SELECT *
    FROM {{ ref('stg_order_reviews') }}

),

ranked_reviews AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY review_answer_timestamp DESC, review_id DESC
        ) AS rn
    FROM reviews

),

aggregated AS (

    SELECT
        order_id,
        COUNT(review_id) AS review_count,
        MIN(review_score) AS worst_review_score,
        MAX(CASE WHEN rn = 1 THEN review_score END) AS latest_review_score,
        MIN(review_creation_date) AS first_review_created_at,
        MAX(review_answer_timestamp) AS last_review_answered_at,
        COALESCE(MIN(review_score) <= 2, false) AS is_negative_review
    FROM ranked_reviews
    GROUP BY order_id

)

SELECT * FROM aggregated
