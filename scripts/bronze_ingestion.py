"""
bronze_ingestion.py
--------------------
Ingests raw data from the three source systems (customers/JSON,
orders/Parquet, products/CSV) into the Bronze layer, with no
transformation applied — only technical metadata columns are added.

Incremental loading is handled by AWS Glue Job Bookmarks (file-level
tracking), so re-running this job only picks up newly arrived files.

Glue Job configuration:
    Glue version:  4.0
    Worker type:   G.1X, 2 workers
    Job bookmark:  Enable
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
from pyspark.sql.functions import current_timestamp, current_date, input_file_name

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

CATALOG = "glue_catalog"
RAW_BASE = "s3://medallion-scd-lakehouse-czs/raw"


def ingest_bronze(*, table_name, file_format, source_path, transformation_ctx, format_options=None):
    print(f"\n[BRONZE] Ingesting {file_format.upper()} from {source_path}")
    print(f"         Target: {table_name}")

    dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format=file_format,
        connection_options={"paths": [source_path], "recurse": True},
        format_options=format_options or {},
        transformation_ctx=transformation_ctx,
    )

    if dyf.count() == 0:
        print("         No new files to process (bookmark up to date).")
        return

    df = dyf.toDF()

    df_bronze = (df
        .withColumn("_ingestion_ts", current_timestamp())
        .withColumn("_ingestion_date", current_date())
        .withColumn("_source_file", input_file_name()))

    df_bronze.writeTo(f"{CATALOG}.{table_name}").append()

    print(f"         Ingestion completed: {df_bronze.count()} rows.")


def main():
    print("=" * 60)
    print("BRONZE INGESTION (multi-format)")
    print("=" * 60)

    ingest_bronze(
        table_name="medallion_bronze.bronze_raw_customers",
        file_format="json",
        source_path=f"{RAW_BASE}/customers/",
        transformation_ctx="bronze_customers_ctx",
    )

    ingest_bronze(
        table_name="medallion_bronze.bronze_raw_orders",
        file_format="parquet",
        source_path=f"{RAW_BASE}/orders/",
        transformation_ctx="bronze_orders_ctx",
    )

    ingest_bronze(
        table_name="medallion_bronze.bronze_raw_products",
        file_format="csv",
        source_path=f"{RAW_BASE}/products/",
        transformation_ctx="bronze_products_ctx",
        format_options={"withHeader": True, "separator": ","},
    )

    print("\nBronze ingestion completed for all sources.")


main()
job.commit()
