"""
gold_aggregations.py
---------------------
Silver -> Gold aggregations: business-ready marts built by joining the
orders fact table against the CURRENT version of each SCD Type 2
dimension (is_current = true).

    gold_agg_sales_by_city      - orders/revenue grouped by customer city
    gold_agg_sales_by_category  - orders/revenue grouped by product category
    gold_ranking_customers      - top 100 customers by total spend

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
from pyspark.sql.functions import col, count, countDistinct, sum as _sum, current_date, row_number
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

CATALOG = "glue_catalog"


def silver_to_gold_sales_city():
    print("\n[GOLD] Sales by city...")

    dim = (spark.table(f"{CATALOG}.medallion_silver.silver_dim_customers_type2")
                .filter(col("is_current") == True))
    fact = spark.table(f"{CATALOG}.medallion_silver.silver_fact_orders")

    df_gold = (fact
        .join(dim, "customer_id", "inner")
        .groupBy("city")
        .agg(
            count("order_id").alias("total_orders"),
            _sum("amount").alias("total_amount"),
        )
        .withColumn("update_date", current_date()))

    df_gold.writeTo(f"{CATALOG}.medallion_gold.gold_agg_sales_by_city").overwritePartitions()
    print(f"Gold agg: {df_gold.count()} cities.")


def silver_to_gold_sales_by_category():
    print("\n[GOLD] Sales by category...")

    dim = (spark.table(f"{CATALOG}.medallion_silver.silver_dim_products_type2")
                .filter(col("is_current") == True))
    fact = spark.table(f"{CATALOG}.medallion_silver.silver_fact_orders")

    df_gold = (fact
        .join(dim, "product_id", "inner")
        .groupBy("category")
        .agg(
            count("order_id").alias("total_orders"),
            _sum("amount").alias("total_amount"),
            countDistinct("product_id").alias("unique_products"),
        )
        .withColumn("update_date", current_date()))

    df_gold.writeTo(f"{CATALOG}.medallion_gold.gold_agg_sales_by_category").overwritePartitions()
    print(f"Gold agg: {df_gold.count()} categories.")


def silver_to_gold_ranking():
    print("\n[GOLD] Customer ranking...")

    dim = (spark.table(f"{CATALOG}.medallion_silver.silver_dim_customers_type2")
                .filter(col("is_current") == True))
    fact = spark.table(f"{CATALOG}.medallion_silver.silver_fact_orders")

    df_join = (fact
        .join(dim, "customer_id", "inner")
        .groupBy("customer_id", "name", "city")
        .agg(_sum("amount").alias("total_amount")))

    w = Window.orderBy(col("total_amount").desc())

    df_gold = (df_join
        .withColumn("ranking", row_number().over(w))
        .orderBy("ranking")
        .limit(100))

    df_gold.writeTo(f"{CATALOG}.medallion_gold.gold_ranking_customers").overwritePartitions()
    print(f"Gold ranking: top {df_gold.count()} customers.")


def main():
    silver_to_gold_sales_city()
    silver_to_gold_sales_by_category()
    silver_to_gold_ranking()
    print("\nGold layer completed.")


main()
job.commit()
