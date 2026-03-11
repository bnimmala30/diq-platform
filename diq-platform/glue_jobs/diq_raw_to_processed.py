import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, DateType
import logging

# ── Setup logging ──────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Initialise Glue and Spark ──────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger.info("Glue job started: diq_raw_to_processed")

# ── Define S3 paths ────────────────────────────────────────────────────────
RAW_PATH       = "s3://diq-lakehouse-prod/raw/transactions/"
PROCESSED_PATH = "s3://diq-lakehouse-prod/processed/transactions/"

# ── Step 1: Read all raw CSV files from S3 ────────────────────────────────
logger.info(f"Reading raw CSV files from: {RAW_PATH}")

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(RAW_PATH)

raw_count = df.count()
logger.info(f"Total raw records loaded: {raw_count}")

# ── Step 2: Standardise column names to lowercase ─────────────────────────
logger.info("Standardising column names...")
df = df.toDF(*[col.lower().strip().replace(" ", "_") for col in df.columns])

# ── Step 3: Fix date format ────────────────────────────────────────────────
logger.info("Fixing date formats...")
df = df.withColumn(
    "transaction_date",
    F.to_date(F.col("transaction_date"), "yyyy-MM-dd")
)

# ── Step 4: Fix data types ─────────────────────────────────────────────────
logger.info("Fixing data types...")
df = df.withColumn("unit_price",  F.col("unit_price").cast(DoubleType()))
df = df.withColumn("quantity",    F.col("quantity").cast(IntegerType()))
df = df.withColumn("total_amount",F.col("total_amount").cast(DoubleType()))

# ── Step 5: Remove rows with null values in critical columns ───────────────
logger.info("Removing rows with missing critical values...")
critical_columns = [
    "transaction_id",
    "transaction_date",
    "shop_id",
    "product_name",
    "unit_price",
    "quantity",
    "total_amount"
]
df = df.dropna(subset=critical_columns)

# ── Step 6: Remove duplicate transactions ─────────────────────────────────
logger.info("Removing duplicate transactions...")
before_dedup = df.count()
df = df.dropDuplicates(["transaction_id"])
after_dedup = df.count()
logger.info(f"Duplicates removed: {before_dedup - after_dedup}")

# ── Step 7: Remove impossible values ──────────────────────────────────────
logger.info("Removing impossible values...")
df = df.filter(F.col("unit_price")   > 0)
df = df.filter(F.col("quantity")     > 0)
df = df.filter(F.col("total_amount") > 0)
df = df.filter(F.col("transaction_date") >= F.lit("2020-01-01"))
df = df.filter(F.col("transaction_date") <= F.current_date())

# ── Step 8: Add useful new columns ────────────────────────────────────────
logger.info("Adding enrichment columns...")

df = df.withColumn("day_of_week",
    F.date_format(F.col("transaction_date"), "EEEE"))

df = df.withColumn("month_name",
    F.date_format(F.col("transaction_date"), "MMMM"))

df = df.withColumn("year",
    F.year(F.col("transaction_date")))

df = df.withColumn("month_number",
    F.month(F.col("transaction_date")))

df = df.withColumn("is_weekend",
    F.when(
        F.dayofweek(F.col("transaction_date")).isin([1, 7]),
        True
    ).otherwise(False))

df = df.withColumn("revenue_category",
    F.when(F.col("total_amount") < 20,  "Low")
     .when(F.col("total_amount") < 100, "Medium")
     .otherwise("High"))

df = df.withColumn("processed_timestamp",
    F.current_timestamp())

# ── Step 9: Final count and log ────────────────────────────────────────────
clean_count = df.count()
logger.info(f"Raw records:   {raw_count}")
logger.info(f"Clean records: {clean_count}")
logger.info(f"Records removed: {raw_count - clean_count}")

# ── Step 10: Write clean data to S3 as Parquet ────────────────────────────
logger.info(f"Writing clean data to: {PROCESSED_PATH}")

df.write \
    .mode("overwrite") \
    .parquet(PROCESSED_PATH)

logger.info("Clean data written successfully!")

# ── Commit Glue job ────────────────────────────────────────────────────────
job.commit()
logger.info("Glue job completed successfully!")
