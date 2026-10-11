WITH date_spine AS (

    SELECT DATEADD('day', SEQ4(), '2016-09-01'::DATE) AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 852))

)

SELECT
    MD5(date_day::VARCHAR) AS date_key,
    date_day,
    DATE_TRUNC('month', date_day)::DATE AS month_start_date,
    WEEKISO(date_day) AS iso_week,
    QUARTER(date_day) AS quarter,
    YEAR(date_day) AS year,
    DAYOFWEEK(date_day) AS day_of_week,
    DAYNAME(date_day) AS day_name,
    MONTHNAME(date_day) AS month_name,
    (date_day >= '2017-01-01' AND date_day <= '2018-08-31') AS is_in_analysis_window
FROM date_spine
