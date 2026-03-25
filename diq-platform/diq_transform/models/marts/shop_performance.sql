-- ============================================================
-- Model: shop_performance
-- Description: Comprehensive shop performance summary
--              Final mart table for dashboard and Claude AI
-- ============================================================

SELECT
    shop_id,
    shop_name,
    city,

    -- Overall revenue metrics
    ROUND(SUM(total_amount), 2)             AS total_revenue,
    COUNT(transaction_id)                   AS total_transactions,
    ROUND(AVG(total_amount), 2)             AS avg_transaction_value,
    ROUND(MAX(total_amount), 2)             AS highest_sale,
    ROUND(MIN(total_amount), 2)             AS lowest_sale,

    -- Customer metrics
    COUNT(DISTINCT customer_id)             AS unique_customers,
    ROUND(
        SUM(total_amount) /
        COUNT(DISTINCT customer_id), 2
    )                                       AS revenue_per_customer,

    -- Product metrics
    COUNT(DISTINCT product_name)            AS unique_products,
    COUNT(DISTINCT category)                AS unique_categories,

    -- Weekend performance
    ROUND(SUM(CASE WHEN is_weekend = TRUE
        THEN total_amount ELSE 0 END), 2)   AS weekend_revenue,
    ROUND(SUM(CASE WHEN is_weekend = FALSE
        THEN total_amount ELSE 0 END), 2)   AS weekday_revenue,
    COUNT(CASE WHEN is_weekend = TRUE
        THEN 1 END)                         AS weekend_transactions,
    COUNT(CASE WHEN is_weekend = FALSE
        THEN 1 END)                         AS weekday_transactions,

    -- Payment method split
    COUNT(CASE WHEN payment_method = 'cash'
        THEN 1 END)                         AS cash_transactions,
    COUNT(CASE WHEN payment_method = 'card'
        THEN 1 END)                         AS card_transactions,
    ROUND(
        COUNT(CASE WHEN payment_method = 'card'
            THEN 1 END) * 100.0 /
        COUNT(transaction_id), 2
    )                                       AS card_payment_pct,

    -- Revenue category breakdown
    COUNT(CASE WHEN revenue_category = 'High'
        THEN 1 END)                         AS high_value_transactions,
    COUNT(CASE WHEN revenue_category = 'Medium'
        THEN 1 END)                         AS medium_value_transactions,
    COUNT(CASE WHEN revenue_category = 'Low'
        THEN 1 END)                         AS low_value_transactions,

    -- Overall ranking
    RANK() OVER (
        ORDER BY SUM(total_amount) DESC
    )                                       AS revenue_rank,

    -- Revenue share
    ROUND(
        SUM(total_amount) * 100.0 /
        SUM(SUM(total_amount)) OVER (), 2
    )                                       AS pct_of_total_revenue,

    CURRENT_TIMESTAMP()                     AS dbt_updated_at

FROM {{ ref('stg_transactions') }}
GROUP BY
    shop_id,
    shop_name,
    city
ORDER BY
    total_revenue DESC
