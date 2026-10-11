WITH date_spine AS (

    SELECT DATEADD('month', SEQ4(), '2016-09-01'::DATE) AS month
    FROM TABLE(GENERATOR(ROWCOUNT => 24))
    WHERE month <= '2018-08-01'::DATE

),

seller_first_month AS (

    SELECT
        seller_id,
        DATE_TRUNC('month', MIN(order_purchase_timestamp))::DATE AS first_active_month
    FROM {{ ref('int_seller_orders_aggregated') }}
    WHERE NOT is_canceled AND NOT is_unavailable
    GROUP BY seller_id

),

dense_spine AS (

    SELECT
        s.seller_id,
        d.month
    FROM seller_first_month s
    INNER JOIN date_spine d
        ON
            s.first_active_month <= d.month
            AND d.month <= '2018-08-01'::DATE

)

SELECT * FROM dense_spine
