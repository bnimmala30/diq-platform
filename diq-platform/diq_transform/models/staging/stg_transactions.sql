-- ============================================================
-- Model: stg_transactions
-- Description: Cleaned and standardised transactions
--              from raw.transactions table
-- ============================================================

SELECT
    transaction_id,
    date                                    AS transaction_date,
    shop_id,
    shop_name,
    city,
    product_name,
    category,
    quantity,
    ROUND(unit_price, 2)                    AS unit_price,
    ROUND(total_amount, 2)                  AS total_amount,
    customer_id,
    payment_method,
    day_of_week,
    month_name,
    year,
    month_number,
    is_weekend,
    revenue_category,
    processed_timestamp,

    -- Derived columns
    CASE
        WHEN is_weekend = TRUE THEN 'Weekend'
        ELSE 'Weekday'
    END                                     AS day_type,

    CASE
        WHEN MONTH(date) IN (12, 1, 2)  THEN 'Summer'
        WHEN MONTH(date) IN (3, 4, 5)   THEN 'Autumn'
        WHEN MONTH(date) IN (6, 7, 8)   THEN 'Winter'
        WHEN MONTH(date) IN (9, 10, 11) THEN 'Spring'
    END                                     AS australian_season,

    CURRENT_TIMESTAMP()                     AS dbt_updated_at

FROM {{ source('raw', 'transactions') }}
WHERE transaction_id IS NOT NULL
  AND total_amount > 0
  AND date IS NOT NULL
