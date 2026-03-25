
-- ============================================================
-- DataIQ360 Snowflake Setup Script
-- Date: 25 March 2026
-- Author: bnimmala30
-- Description: Complete Snowflake setup for DataIQ360
-- ============================================================

-- ── Step 1: Create Database ──────────────────────────────────
CREATE DATABASE IF NOT EXISTS dataiq360_db
COMMENT = 'DataIQ360 retail analytics platform database';

-- ── Step 2: Create Schemas ───────────────────────────────────
CREATE SCHEMA IF NOT EXISTS dataiq360_db.raw
COMMENT = 'Raw data loaded directly from S3 - never modified';

CREATE SCHEMA IF NOT EXISTS dataiq360_db.analytics
COMMENT = 'Cleaned and transformed data - built by dbt models';

CREATE SCHEMA IF NOT EXISTS dataiq360_db.marts
COMMENT = 'Final analytics tables - used by dashboard and AI';

-- ── Step 3: Create Virtual Warehouse ─────────────────────────
CREATE WAREHOUSE IF NOT EXISTS diq_warehouse
WITH
    WAREHOUSE_SIZE      = 'X-SMALL'
    AUTO_SUSPEND        = 60
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'DataIQ360 compute warehouse - auto suspends after 60 seconds';

-- ── Step 4: Create RBAC Roles ─────────────────────────────────
CREATE ROLE IF NOT EXISTS diq_admin_role
COMMENT = 'Full access to all DataIQ360 objects';

CREATE ROLE IF NOT EXISTS diq_transform_role
COMMENT = 'Used by dbt to transform raw data into analytics';

CREATE ROLE IF NOT EXISTS diq_loader_role
COMMENT = 'Used by Snowpipe to load data from S3 into raw schema';

CREATE ROLE IF NOT EXISTS diq_reporting_role
COMMENT = 'Read only access to marts schema for dashboard and AI';

-- ── Step 5: Grant Permissions ─────────────────────────────────
GRANT USAGE ON WAREHOUSE diq_warehouse TO ROLE diq_admin_role;
GRANT USAGE ON WAREHOUSE diq_warehouse TO ROLE diq_transform_role;
GRANT USAGE ON WAREHOUSE diq_warehouse TO ROLE diq_loader_role;
GRANT USAGE ON WAREHOUSE diq_warehouse TO ROLE diq_reporting_role;

GRANT USAGE ON DATABASE dataiq360_db TO ROLE diq_admin_role;
GRANT USAGE ON DATABASE dataiq360_db TO ROLE diq_transform_role;
GRANT USAGE ON DATABASE dataiq360_db TO ROLE diq_loader_role;
GRANT USAGE ON DATABASE dataiq360_db TO ROLE diq_reporting_role;

GRANT ALL ON SCHEMA dataiq360_db.raw TO ROLE diq_admin_role;
GRANT ALL ON SCHEMA dataiq360_db.analytics TO ROLE diq_admin_role;
GRANT ALL ON SCHEMA dataiq360_db.marts TO ROLE diq_admin_role;

GRANT USAGE ON SCHEMA dataiq360_db.raw TO ROLE diq_transform_role;
GRANT ALL ON SCHEMA dataiq360_db.analytics TO ROLE diq_transform_role;

GRANT ALL ON SCHEMA dataiq360_db.raw TO ROLE diq_loader_role;

GRANT USAGE ON SCHEMA dataiq360_db.marts TO ROLE diq_reporting_role;

GRANT ROLE diq_admin_role TO USER bhavani30;
GRANT ROLE diq_transform_role TO USER bhavani30;
GRANT ROLE diq_loader_role TO USER bhavani30;
GRANT ROLE diq_reporting_role TO USER bhavani30;

-- ── Step 6: Create Storage Integration ───────────────────────
USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION IF NOT EXISTS diq_s3_integration
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = 'S3'
    ENABLED                   = TRUE
    STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::221471234562:role/diq-snowflake-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://diq-lakehouse-prod/processed/transactions/')
COMMENT = 'Secure integration between Snowflake and S3';

-- ── Step 7: Create File Format ────────────────────────────────
USE ROLE diq_admin_role;
USE DATABASE dataiq360_db;
USE SCHEMA raw;

CREATE FILE FORMAT IF NOT EXISTS dataiq360_db.raw.diq_parquet_format
    TYPE               = PARQUET
    SNAPPY_COMPRESSION = TRUE
COMMENT = 'Parquet file format for DataIQ360 S3 files';

-- ── Step 8: Create External Stage ────────────────────────────
CREATE STAGE IF NOT EXISTS dataiq360_db.raw.diq_s3_stage
    URL                 = 's3://diq-lakehouse-prod/processed/transactions/'
    STORAGE_INTEGRATION = diq_s3_integration
    FILE_FORMAT         = (TYPE = PARQUET SNAPPY_COMPRESSION = TRUE)
COMMENT = 'External stage pointing to S3 processed transactions folder';

-- ── Step 9: Create Transactions Table ────────────────────────
CREATE TABLE IF NOT EXISTS dataiq360_db.raw.transactions (
    transaction_id      VARCHAR(20),
    date                DATE,
    time                VARCHAR(50),
    shop_id             VARCHAR(10),
    shop_name           VARCHAR(100),
    city                VARCHAR(50),
    product_name        VARCHAR(100),
    category            VARCHAR(50),
    quantity            INTEGER,
    unit_price          FLOAT,
    total_amount        FLOAT,
    customer_id         VARCHAR(20),
    payment_method      VARCHAR(20),
    day_of_week         VARCHAR(20),
    month_name          VARCHAR(20),
    year                INTEGER,
    month_number        INTEGER,
    is_weekend          BOOLEAN,
    revenue_category    VARCHAR(10),
    processed_timestamp TIMESTAMP
)
COMMENT = 'Clean retail transactions loaded from S3';

-- ── Step 10: Create Staging Table ────────────────────────────
CREATE TABLE IF NOT EXISTS dataiq360_db.raw.transactions_raw_stage (
    raw_data VARIANT
)
COMMENT = 'Temporary staging table for Parquet loading';

-- ── Step 11: Load Data ────────────────────────────────────────
COPY INTO dataiq360_db.raw.transactions_raw_stage
FROM @dataiq360_db.raw.diq_s3_stage
FILE_FORMAT = dataiq360_db.raw.diq_parquet_format
ON_ERROR = CONTINUE;

INSERT INTO dataiq360_db.raw.transactions
SELECT
    raw_data:transaction_id::VARCHAR(20),
    raw_data:date::DATE,
    raw_data:time::VARCHAR(50),
    raw_data:shop_id::VARCHAR(10),
    raw_data:shop_name::VARCHAR(100),
    raw_data:city::VARCHAR(50),
    raw_data:product_name::VARCHAR(100),
    raw_data:category::VARCHAR(50),
    raw_data:quantity::INTEGER,
    raw_data:unit_price::FLOAT,
    raw_data:total_amount::FLOAT,
    raw_data:customer_id::VARCHAR(20),
    raw_data:payment_method::VARCHAR(20),
    raw_data:day_of_week::VARCHAR(20),
    raw_data:month_name::VARCHAR(20),
    raw_data:year::INTEGER,
    raw_data:month_number::INTEGER,
    raw_data:is_weekend::BOOLEAN,
    raw_data:revenue_category::VARCHAR(10),
    raw_data:processed_timestamp::TIMESTAMP
FROM dataiq360_db.raw.transactions_raw_stage;

-- ── Analytics Queries ─────────────────────────────────────────
-- Revenue by shop
SELECT
    shop_name,
    ROUND(SUM(total_amount), 2)  AS total_revenue,
    ROUND(AVG(total_amount), 2)  AS avg_sale_value,
    COUNT(transaction_id)         AS total_transactions
FROM dataiq360_db.raw.transactions
GROUP BY shop_name
ORDER BY total_revenue DESC;

-- Revenue by day of week
SELECT
    day_of_week,
    ROUND(SUM(total_amount), 2) AS revenue,
    COUNT(transaction_id)        AS transactions
FROM dataiq360_db.raw.transactions
GROUP BY day_of_week
ORDER BY revenue DESC;
