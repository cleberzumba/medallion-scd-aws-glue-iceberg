"""
create_tables.py
-----------------
Creates the 12 Iceberg tables of the Medallion architecture (Bronze / Silver /
Gold) in the AWS Glue Data Catalog.

This is a one-time (or occasional) DDL/bootstrap step. It deliberately does
NOT run inside the Step Functions pipeline: schema changes are a different
kind of operation than a recurring data load, and mixing the two is an
anti-pattern. Run this manually whenever the environment is provisioned from
scratch, or the schema changes.

Catalog:   glue_catalog (Spark session catalog backed by the AWS Glue Data
           Catalog, configured via Glue Job parameters — see README).
Databases: medallion_bronze, medallion_silver, medallion_gold
           (created beforehand in Lake Formation, with LOCATION pointing to
           s3://<bucket>/warehouse/<layer>/)

Tables created:
    BRONZE
       ├─ bronze_raw_customers
       ├─ bronze_raw_orders
       └─ bronze_raw_products
    SILVER
       ├─ silver_stg_customers
       ├─ silver_stg_products
       ├─ silver_dim_customers_type1  (SCD Type 1)
       ├─ silver_dim_customers_type2  (SCD Type 2 — address history)
       ├─ silver_dim_products_type2   (SCD Type 2 — price history)
       └─ silver_fact_orders
    GOLD
       ├─ gold_agg_sales_by_city
       ├─ gold_agg_sales_by_category
       └─ gold_ranking_customers

Glue Job configuration:
    Glue version:  4.0
    Worker type:   G.1X, 2 workers
    Job bookmark:  Disable
    Job parameters:
        --datalake-formats = iceberg
        --conf = spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
                 --conf spark.sql.catalog.glue_catalog.warehouse=s3://<bucket>/warehouse/
                 --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
                 --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
                 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

CATALOG = "glue_catalog"


# =========================================================
# BRONZE — raw ingestion, no transformation (everything as string)
# =========================================================
def create_bronze_customers():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.medallion_bronze.bronze_raw_customers (
            customer_id     STRING,
            name            STRING,
            city            STRING,
            event_date      STRING,
            _ingestion_ts   TIMESTAMP,
            _ingestion_date DATE,
            _source_file    STRING
        )
        USING iceberg
        PARTITIONED BY (_ingestion_date)
    """)
    print("bronze_raw_customers ready.")


def create_bronze_orders():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.medallion_bronze.bronze_raw_orders (
            order_id        STRING,
            customer_id     STRING,
            product_id      STRING,
            amount          STRING,
            order_date      STRING,
            _ingestion_ts   TIMESTAMP,
            _ingestion_date DATE,
            _source_file    STRING
        )
        USING iceberg
        PARTITIONED BY (_ingestion_date)
    """)
    print("bronze_raw_orders ready.")


def create_bronze_products():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.medallion_bronze.bronze_raw_products (
            product_id      STRING,
            product_name    STRING,
            category        STRING,
            price           STRING,
            launch_date     STRING,
            _ingestion_ts   TIMESTAMP,
            _ingestion_date DATE,
            _source_file    STRING
        )
        USING iceberg
        PARTITIONED BY (_ingestion_date)
    """)
    print("bronze_raw_products ready.")


# =========================================================
# SILVER — cleansed staging + SCD dimensions + fact
# =========================================================
def create_silver_stg_customers():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.medallion_silver.silver_stg_customers (
            customer_id INT,
            name        STRING,
            city        STRING,
            event_date  DATE
        )
        USING iceberg
    """)
    print("silver_stg_customers ready.")


def create_silver_stg_products():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.medallion_silver.silver_stg_products (
            product_id   STRING,
            product_name STRING,
            category     STRING,
            price        DOUBLE,
            launch_date  DATE
        )
        USING iceberg
    """)
    print("silver_stg_products ready.")


def create_silver_dim_customers_type1():
    # SCD Type 1 — overwrite in place, no history preserved.
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.medallion_silver.silver_dim_customers_type1 (
            customer_id INT,
            name        STRING,
            city        STRING,
            update_date DATE
        )
        USING iceberg
    """)
    print("silver_dim_customers_type1 ready.")


def create_silver_dim_customers_type2():
    # SCD Type 2 — versioned, preserves full address history.
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.medallion_silver.silver_dim_customers_type2 (
            customer_id INT,
            name        STRING,
            city        STRING,
            valid_from  DATE,
            valid_to    DATE,
            is_current  BOOLEAN
        )
        USING iceberg
    """)
    print("silver_dim_customers_type2 ready.")


def create_silver_dim_products_type2():
    # SCD Type 2 — versioned, tracks price history for point-in-time joins.
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.medallion_silver.silver_dim_products_type2 (
            product_id   STRING,
            product_name STRING,
            category     STRING,
            price        DOUBLE,
            valid_from   DATE,
            valid_to     DATE,
            is_current   BOOLEAN
        )
        USING iceberg
    """)
    print("silver_dim_products_type2 ready.")


def create_silver_fact_orders():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.medallion_silver.silver_fact_orders (
            order_id    INT,
            customer_id INT,
            product_id  STRING,
            amount      DOUBLE,
            order_date  DATE,
            year        INT,
            month       INT
        )
        USING iceberg
        PARTITIONED BY (year, month)
    """)
    print("silver_fact_orders ready.")


# =========================================================
# GOLD — business aggregations
# =========================================================
def create_gold_agg_sales_by_city():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.medallion_gold.gold_agg_sales_by_city (
            city         STRING,
            total_orders INT,
            total_amount DOUBLE,
            update_date  DATE
        )
        USING iceberg
    """)
    print("gold_agg_sales_by_city ready.")


def create_gold_agg_sales_by_category():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.medallion_gold.gold_agg_sales_by_category (
            category        STRING,
            total_orders    INT,
            total_amount    DOUBLE,
            unique_products INT,
            update_date     DATE
        )
        USING iceberg
    """)
    print("gold_agg_sales_by_category ready.")


def create_gold_ranking_customers():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.medallion_gold.gold_ranking_customers (
            customer_id  INT,
            name         STRING,
            city         STRING,
            total_amount DOUBLE,
            ranking      INT
        )
        USING iceberg
    """)
    print("gold_ranking_customers ready.")


def main():
    print("Creating Iceberg tables across Bronze / Silver / Gold...")

    create_bronze_customers()
    create_bronze_orders()
    create_bronze_products()

    create_silver_stg_customers()
    create_silver_stg_products()
    create_silver_dim_customers_type1()
    create_silver_dim_customers_type2()
    create_silver_dim_products_type2()
    create_silver_fact_orders()

    create_gold_agg_sales_by_city()
    create_gold_agg_sales_by_category()
    create_gold_ranking_customers()

    print("All 12 Iceberg tables created.")


main()
job.commit()
