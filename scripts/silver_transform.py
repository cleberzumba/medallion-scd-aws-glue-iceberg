"""
silver_transform.py
--------------------
Bronze -> Silver transformation: cleansing, typing, and SCD dimension
maintenance (Type 1 and Type 2 for customers, Type 2 for products),
plus the orders fact table.

BATCH-ORIENTED, CHRONOLOGICAL PROCESSING
    Each source file carries a YYYY-MM token in its file name (the
    "batch key"). This job discovers every batch present in Bronze,
    sorts them chronologically, and processes them ONE AT A TIME, in
    order — this is what makes SCD Type 2 produce a real historical
    version chain instead of a single, corrupted "latest wins" state.

IDEMPOTENCY
    Glue Job Bookmarks (used in Bronze) work at file level and do not
    align with this job's batch-level processing semantics. Instead,
    a dedicated control table (silver_batch_control) records which
    batch keys have already been applied to the SCD dimensions. On
    every run, already-processed batches are skipped — safe to rerun
    the Step Functions pipeline without corrupting SCD history or
    duplicating fact rows (writes use overwritePartitions()).

    NOTE: this table is what stands between a correct pipeline and a
    real production incident. An earlier version of this job had no
    such control and reprocessed every batch on every run; replaying
    an old batch after the dimension's "current" pointer had already
    advanced made the SCD2 change-detection logic misinterpret old
    data as a new change, corrupting the version history. See
    docs/lake_formation_setup.md and the README for details.

Glue Job configuration:
    Glue version:  4.0
    Worker type:   G.1X, 2 workers
    Job bookmark:  Disable (idempotency is handled by silver_batch_control)
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

# NOTE: Spark SQL string literals silently strip single backslashes
# (unlike Presto/Athena), so `\d` becomes `d` and the regex breaks
# unless the backslash is doubled.
BATCH_KEY_REGEX = r"(\\d{4}-\\d{2})"


# =========================================================
# IDEMPOTENCY — batch control table
# =========================================================
def ensure_batch_control_table():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.medallion_silver.silver_batch_control (
            batch_key    STRING,
            processed_at TIMESTAMP
        )
        USING iceberg
    """)


def mark_batch_processed(batch_key):
    spark.sql(f"""
        INSERT INTO {CATALOG}.medallion_silver.silver_batch_control
        VALUES ('{batch_key}', current_timestamp())
    """)


# =========================================================
# SILVER — cleansing and typing
# =========================================================
def bronze_to_silver_customers(batch_key):
    print(f"\n[SILVER] Bronze -> Silver (customers) - batch {batch_key}...")

    df_bronze = (spark.table(f"{CATALOG}.medallion_bronze.bronze_raw_customers")
                       .filter(f"_source_file LIKE '%{batch_key}%'"))

    df_silver = (df_bronze
        .selectExpr(
            "CAST(customer_id AS INT) AS customer_id",
            "name",
            "city",
            "TO_DATE(event_date, 'yyyy-MM-dd') AS event_date",
        )
        .filter("customer_id IS NOT NULL")
        .filter("name IS NOT NULL")
        .dropna(subset=["city", "event_date"])
        .dropDuplicates(["customer_id"]))

    df_silver.writeTo(f"{CATALOG}.medallion_silver.silver_stg_customers").overwritePartitions()

    print(f"Silver stg: {df_silver.count()} customers after cleaning.")


def bronze_to_silver_products(batch_key):
    print(f"\n[SILVER] Bronze -> Silver (products) - batch {batch_key}...")

    df_bronze = (spark.table(f"{CATALOG}.medallion_bronze.bronze_raw_products")
                       .filter(f"_source_file LIKE '%{batch_key}%'"))

    df_silver = (df_bronze
        .selectExpr(
            "product_id",
            "product_name",
            "category",
            "CAST(price AS DOUBLE) AS price",
            "TO_DATE(launch_date, 'yyyy-MM-dd') AS launch_date",
        )
        .filter("product_id IS NOT NULL")
        .filter("price > 0")
        .dropDuplicates(["product_id"]))

    df_silver.writeTo(f"{CATALOG}.medallion_silver.silver_stg_products").overwritePartitions()

    print(f"Silver stg: {df_silver.count()} products after cleaning.")


def bronze_to_silver_orders():
    print("\n[SILVER] Bronze -> Silver (orders)...")

    df_bronze = spark.table(f"{CATALOG}.medallion_bronze.bronze_raw_orders")

    df_silver = (df_bronze
        .selectExpr(
            "CAST(order_id AS INT)    AS order_id",
            "CAST(customer_id AS INT) AS customer_id",
            "product_id",
            "CAST(amount AS DOUBLE)   AS amount",
            "TO_DATE(order_date)      AS order_date",
        )
        .fillna({"amount": 0.0})
        .filter("order_id IS NOT NULL")
        .filter("product_id IS NOT NULL")
        .filter("amount > 0")
        .dropDuplicates(["order_id"])
        .selectExpr("*", "YEAR(order_date) AS year", "MONTH(order_date) AS month"))

    df_silver.writeTo(f"{CATALOG}.medallion_silver.silver_fact_orders").overwritePartitions()

    print(f"Silver fact: {df_silver.count()} orders processed.")


# =========================================================
# SILVER — SCD dimensions
# =========================================================
def apply_scd_type1():
    print("\n[SILVER] Applying SCD Type 1...")

    spark.sql(f"""
        MERGE INTO {CATALOG}.medallion_silver.silver_dim_customers_type1 target
        USING {CATALOG}.medallion_silver.silver_stg_customers source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN UPDATE SET
            target.name        = source.name,
            target.city        = source.city,
            target.update_date = source.event_date
        WHEN NOT MATCHED THEN INSERT (customer_id, name, city, update_date)
        VALUES (source.customer_id, source.name, source.city, source.event_date)
    """)

    print("SCD Type 1 applied.")


def apply_scd_type2():
    print("\n[SILVER] Applying SCD Type 2 (customers)...")

    df_changes = spark.sql(f"""
        WITH current_dim AS (
            SELECT customer_id, name, city
            FROM {CATALOG}.medallion_silver.silver_dim_customers_type2
            WHERE is_current = true
        )
        SELECT s.customer_id, s.name, s.city, s.event_date
        FROM {CATALOG}.medallion_silver.silver_stg_customers s
        LEFT JOIN current_dim c ON s.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
           OR s.name <> c.name
           OR s.city <> c.city
    """)

    change_count = df_changes.count()
    if change_count == 0:
        print("No customer changes detected.")
        return

    df_changes.createOrReplaceTempView("changes")

    spark.sql(f"""
        MERGE INTO {CATALOG}.medallion_silver.silver_dim_customers_type2 target
        USING (
            SELECT c.customer_id, c.event_date
            FROM changes c
            INNER JOIN {CATALOG}.medallion_silver.silver_dim_customers_type2 d
                ON c.customer_id = d.customer_id
                AND d.is_current = true
        ) source
        ON target.customer_id = source.customer_id
           AND target.is_current = true
        WHEN MATCHED THEN UPDATE SET
            target.valid_to   = source.event_date,
            target.is_current = false
    """)

    spark.sql(f"""
        INSERT INTO {CATALOG}.medallion_silver.silver_dim_customers_type2
        SELECT
            customer_id, name, city,
            event_date AS valid_from,
            NULL       AS valid_to,
            true       AS is_current
        FROM changes
    """)

    print(f"SCD Type 2 customers: {change_count} version(s) processed.")


def apply_scd_type2_products(business_date):
    print(f"\n[SILVER] Applying SCD Type 2 (products) - as of {business_date}...")

    df_changes = spark.sql(f"""
        WITH current_dim AS (
            SELECT product_id, product_name, category, price
            FROM {CATALOG}.medallion_silver.silver_dim_products_type2
            WHERE is_current = true
        )
        SELECT s.product_id, s.product_name, s.category, s.price, s.launch_date
        FROM {CATALOG}.medallion_silver.silver_stg_products s
        LEFT JOIN current_dim c ON s.product_id = c.product_id
        WHERE c.product_id IS NULL
           OR s.product_name <> c.product_name
           OR s.category     <> c.category
           OR s.price        <> c.price
    """)

    change_count = df_changes.count()
    if change_count == 0:
        print("No product changes detected.")
        return

    df_changes.createOrReplaceTempView("product_changes")

    spark.sql(f"""
        MERGE INTO {CATALOG}.medallion_silver.silver_dim_products_type2 target
        USING (
            SELECT c.product_id, DATE('{business_date}') AS change_date
            FROM product_changes c
            INNER JOIN {CATALOG}.medallion_silver.silver_dim_products_type2 d
                ON c.product_id = d.product_id
                AND d.is_current = true
        ) source
        ON target.product_id = source.product_id
           AND target.is_current = true
        WHEN MATCHED THEN UPDATE SET
            target.valid_to   = source.change_date,
            target.is_current = false
    """)

    spark.sql(f"""
        INSERT INTO {CATALOG}.medallion_silver.silver_dim_products_type2
        SELECT
            product_id, product_name, category, price,
            DATE('{business_date}') AS valid_from,
            NULL                    AS valid_to,
            true                    AS is_current
        FROM product_changes
    """)

    print(f"SCD Type 2 products: {change_count} version(s) processed.")


# =========================================================
# ORCHESTRATION — one batch at a time, chronological order,
# skipping batches already applied (idempotent reruns)
# =========================================================
def discover_batches():
    query = f"""
        SELECT DISTINCT regexp_extract(_source_file, '{BATCH_KEY_REGEX}', 1) AS batch_key
        FROM {CATALOG}.medallion_bronze.bronze_raw_customers
        WHERE regexp_extract(_source_file, '{BATCH_KEY_REGEX}', 1) <> ''
        ORDER BY batch_key
    """
    rows = spark.sql(query).collect()

    if not rows:
        raise ValueError(
            "No batch could be identified in bronze_raw_customers. Check that "
            "_source_file is populated and file names carry a YYYY-MM segment."
        )

    all_batches = [(r.batch_key, f"{r.batch_key}-01") for r in rows]

    processed = {
        r.batch_key
        for r in spark.sql(
            f"SELECT batch_key FROM {CATALOG}.medallion_silver.silver_batch_control"
        ).collect()
    }

    pending = [(bk, bd) for bk, bd in all_batches if bk not in processed]

    print(f"Batches found in Bronze: {[b for b, _ in all_batches]}")
    print(f"Already processed (skipped): {sorted(processed)}")
    print(f"Pending this run: {[b for b, _ in pending]}")

    return pending


def process_batch(batch_key, business_date):
    print("\n" + "=" * 60)
    print(f"BATCH {batch_key} (business date {business_date})")
    print("=" * 60)

    bronze_to_silver_customers(batch_key)
    bronze_to_silver_products(batch_key)

    apply_scd_type1()
    apply_scd_type2()
    apply_scd_type2_products(business_date)

    mark_batch_processed(batch_key)


def main():
    ensure_batch_control_table()

    pending_batches = discover_batches()

    if not pending_batches:
        print("No pending batches. Silver dimensions are already up to date.")
    else:
        for batch_key, business_date in pending_batches:
            process_batch(batch_key, business_date)

    print("\n" + "=" * 60)
    print("FACT TABLE")
    print("=" * 60)
    bronze_to_silver_orders()

    print("\nSilver layer completed.")


main()
job.commit()
