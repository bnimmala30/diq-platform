-- ============================================================
-- Model: daily_revenue
-- Description: Daily revenue aggregated by shop and date
--              Used by Claude AI for trend analysis
-- ============================================================

SELECT
    transaction_date,
    shop_id,
    shop_name,
    city,
    day_of_week,
    day_type,
    australian_season,
    month_name,
    month_number,
    year,
    is_weekend,

    -- Revenue metrics
    ROUND(SUM(total_amount), 2)             AS daily_revenue,
    COUNT(transaction_id)                   AS total_transactions,
    ROUND(AVG(total_amount), 2)             AS avg_transaction_value,
    ROUND(MAX(total_amount), 2)             AS max_transaction_value,
    ROUND(MIN(total_amount), 2)             AS min_transaction_value,

    -- Product metrics
    COUNT(DISTINCT product_name)            AS unique_products_sold,
    COUNT(DISTINCT customer_id)             AS unique_customers,

    -- Payment metrics
    SUM(CASE WHEN payment_method = 'cash'
        THEN 1 ELSE 0 END)                  AS cash_transactions,
    SUM(CASE WHEN payment_method = 'card'
        THEN 1 ELSE 0 END)                  AS card_transactions,

    CURRENT_TIMESTAMP()                     AS dbt_updated_at

FROM {{ ref('stg_transactions') }}
GROUP BY
    transaction_date,
    shop_id,
    shop_name,
    city,
    day_of_week,
    day_type,
    australian_season,
    month_name,
    month_number,
    year,
    is_weekend
ORDER BY
    transaction_date DESC,
    shop_id
