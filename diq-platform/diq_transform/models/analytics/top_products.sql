-- ============================================================
-- Model: top_products
-- Description: Product performance analysis by shop
--              Used by Claude AI for product insights
-- ============================================================

SELECT
    shop_id,
    shop_name,
    city,
    product_name,
    category,

    -- Sales metrics
    COUNT(transaction_id)                   AS total_transactions,
    SUM(quantity)                           AS total_units_sold,
    ROUND(SUM(total_amount), 2)             AS total_revenue,
    ROUND(AVG(total_amount), 2)             AS avg_sale_value,
    ROUND(AVG(unit_price), 2)               AS avg_unit_price,

    -- Performance rankings
    RANK() OVER (
        PARTITION BY shop_id
        ORDER BY SUM(total_amount) DESC
    )                                       AS revenue_rank_in_shop,

    RANK() OVER (
        PARTITION BY shop_id
        ORDER BY COUNT(transaction_id) DESC
    )                                       AS popularity_rank_in_shop,

    -- Weekend vs weekday split
    SUM(CASE WHEN is_weekend = TRUE
        THEN total_amount ELSE 0 END)       AS weekend_revenue,
    SUM(CASE WHEN is_weekend = FALSE
        THEN total_amount ELSE 0 END)       AS weekday_revenue,

    -- Revenue percentage of shop total
    ROUND(
        SUM(total_amount) * 100.0 /
        SUM(SUM(total_amount)) OVER (
            PARTITION BY shop_id
        ), 2
    )                                       AS pct_of_shop_revenue,

    CURRENT_TIMESTAMP()                     AS dbt_updated_at

FROM {{ ref('stg_transactions') }}
GROUP BY
    shop_id,
    shop_name,
    city,
    product_name,
    category
ORDER BY
    shop_id,
    total_revenue DESC
