# Medallion Lakehouse on AWS — Glue, Iceberg, Lake Formation, Step Functions

A Medallion (Bronze / Silver / Gold) lakehouse pipeline built entirely with
AWS-native services: AWS Glue (PySpark) for transformation, Apache Iceberg
as the table format, AWS Glue Data Catalog + Lake Formation for governance,
Step Functions for orchestration, and Athena for SQL access.

This project re-implements the same business rule as a companion project
built on Azure Databricks + Delta Lake
([medallion-scd-pyspark](https://github.com/cleberzumba/medallion-scd-pyspark)),
but as an independent AWS engineering exercise — not a port. Every design
decision here (naming, partitioning, orchestration, governance model) was
made on its own merits for this stack.

## Overview

Three source systems, three formats, one lakehouse:

| Source      | Format  | Business role                                   |
|-------------|---------|--------------------------------------------------|
| `customers` | JSON    | Customer dimension — SCD Type 1 and Type 2        |
| `products`  | CSV     | Product dimension — SCD Type 2 (price history)    |
| `orders`    | Parquet | Transaction fact table                            |

Data flows through three layers:

- **Bronze** — raw ingestion, no transformation, one Iceberg table per
  source, technical metadata columns added.
- **Silver** — cleansed, typed data; SCD Type 1 and Type 2 dimensions;
  the orders fact table.
- **Gold** — business aggregations: sales by city, sales by category,
  customer ranking.

Processing is **batch-oriented and strictly chronological**: each load is
tied to a `YYYY-MM` batch key, and batches are always applied in order.
This is what allows SCD Type 2 to build a real, correct version history
instead of a single "last write wins" snapshot.

## Architecture

![Architecture diagram: S3 raw data flows through Bronze, Silver and Gold AWS Glue Jobs into Apache Iceberg tables on S3, orchestrated by AWS Step Functions, cataloged and governed by AWS Glue Data Catalog and Lake Formation, and queried through Amazon Athena](docs/architecture.svg)

### AWS services used

| Concern              | Service                         |
|-----------------------|----------------------------------|
| Storage                | Amazon S3                       |
| Transformation          | AWS Glue Jobs (PySpark)         |
| Table format            | Apache Iceberg                  |
| Catalog                 | AWS Glue Data Catalog           |
| Fine-grained governance | AWS Lake Formation               |
| Orchestration            | AWS Step Functions              |
| SQL / analytics          | Amazon Athena                   |
| Access control            | IAM Roles + Lake Formation grants |

### S3 layout

```
s3://medallion-scd-lakehouse-czs/
├── raw/
│   ├── customers/      # JSON source files
│   ├── orders/         # Parquet source files
│   └── products/       # CSV source files
├── warehouse/
│   ├── bronze/         # Iceberg tables — medallion_bronze database
│   ├── silver/         # Iceberg tables — medallion_silver database
│   └── gold/           # Iceberg tables — medallion_gold database
└── athena-results/     # Athena query output location
```

## Data model — 12 Iceberg tables

**Bronze** (raw, 1:1 with source, partitioned by `_ingestion_date`)
- `bronze_raw_customers`
- `bronze_raw_orders`
- `bronze_raw_products`

**Silver**
- `silver_stg_customers`, `silver_stg_products` — cleansed staging
- `silver_dim_customers_type1` — customer dimension, SCD Type 1 (overwrite)
- `silver_dim_customers_type2` — customer dimension, SCD Type 2 (full
  history of name/city changes)
- `silver_dim_products_type2` — product dimension, SCD Type 2 (price
  history)
- `silver_fact_orders` — orders fact table, partitioned by `(year, month)`
- `silver_batch_control` — internal control table; tracks which batches
  have already been applied, making the Silver job idempotent

**Gold**
- `gold_agg_sales_by_city`
- `gold_agg_sales_by_category`
- `gold_ranking_customers` — top 100 customers by total spend

## Repository structure

```
.
├── README.md
├── scripts/
│   ├── create_tables.py       # One-time DDL bootstrap (not in the pipeline)
│   ├── bronze_ingestion.py    # Raw ingestion, Job Bookmarks
│   ├── silver_transform.py    # Batch-oriented SCD1/SCD2 + fact table
│   └── gold_aggregations.py   # Business aggregations
├── step_functions/
│   └── medallion_scd_pipeline.asl.json   # Orchestration state machine
├── sql/
│   └── validation_queries.sql # All Athena validation/analysis queries
└── docs/
    └── lake_formation_setup.md   # Governance setup + a real gotcha found
```

## Pipeline flow

The Step Functions state machine `medallion-scd-pipeline` (Standard
workflow) runs the three Glue Jobs in sequence, waiting for each to finish
before starting the next (`glue:startJobRun.sync`):

```
BronzeIngestion  ──▶  SilverTransform  ──▶  GoldAggregations
```

`create_tables.py` is intentionally **not** part of this state machine.
Schema creation is a one-time/occasional bootstrap concern, not a recurring
data load, and mixing the two is an anti-pattern — it is run manually
whenever the environment is provisioned from scratch or the schema changes.

## Batch processing and idempotency

Each source file name carries a `YYYY-MM` batch key. `silver_transform.py`:

1. Discovers every batch key present in Bronze.
2. Sorts them chronologically.
3. Skips any batch already recorded in `silver_batch_control`.
4. Processes the remaining batches **one at a time, in order**, applying
   SCD Type 1 and Type 2 logic and recording each processed batch in
   `silver_batch_control` as it completes.

This matters because SCD Type 2 change-detection compares each new batch
against the *current* version of the dimension. Processing batches out of
order — or reprocessing an old batch after the dimension has already
advanced — causes the MERGE logic to misinterpret old data as a new change,
corrupting the version history. `silver_batch_control` is what makes it
safe to re-run the full Step Functions pipeline without side effects: a
second run over the same data is a no-op.

## Governance

Access is governed by AWS Lake Formation, with three principals:

- `cleber-admin` (IAM user) — administrator, full access.
- `medallion-glue-role` (IAM role) — assumed by all Glue Jobs.
- `cleber-analyst` (IAM user) — read-only, restricted to `medallion_silver`
  and `medallion_gold` (no access to `medallion_bronze`), with Lake
  Formation Data Filters enforcing:
  - **Row-level security**: only `is_current = true` rows are visible on
    SCD Type 2 dimensions.
  - **Column-level security**: internal tracking columns excluded from the
    analyst's view.

Full setup notes, including a real governance gap that was found and fixed
during validation (Lake Formation's legacy `IAMAllowedPrincipals` grants
silently bypassing all fine-grained permissions), are documented in
[`docs/lake_formation_setup.md`](docs/lake_formation_setup.md).

## Setup / reproduction

1. Create an S3 bucket and the `raw/`, `warehouse/`, `athena-results/`
   prefixes shown above; upload sample source files under `raw/`.
2. Create the `medallion-glue-role` IAM role (Glue service trust,
   permissions for S3, Glue Data Catalog, and Lake Formation data access).
3. Create the `medallion_bronze`, `medallion_silver`, `medallion_gold`
   databases in Lake Formation, with `LOCATION` set to their respective
   `warehouse/<layer>/` prefixes; register the S3 location using the
   `AWSServiceRoleForLakeFormationDataAccess` service-linked role.
4. Grant `Super` on all three databases to `cleber-admin` and
   `medallion-glue-role`.
5. Create the four Glue Jobs (`create_tables`, `medallion-bronze-ingestion`,
   `medallion-silver-transform`, `medallion-gold-aggregations`) from the
   scripts in `scripts/`, using Glue 4.0, G.1X x2 workers, and the Iceberg
   job parameters documented at the top of each script.
6. Run `create_tables.py` once to create the 12 Iceberg tables.
7. Create the `medallion-scd-pipeline` Standard Step Functions state
   machine from `step_functions/medallion_scd_pipeline.asl.json`.
8. Run the state machine. Validate results with the queries in
   `sql/validation_queries.sql` via Amazon Athena.
9. (Optional) Configure the `cleber-analyst` read-only user and Lake
   Formation Data Filters as described in
   [`docs/lake_formation_setup.md`](docs/lake_formation_setup.md).

## Design decisions

- **Iceberg over Delta Lake**: this is the AWS-native table format with
  first-class Glue/Athena/Lake Formation integration, avoiding the need for
  a third-party connector.
- **`glue_catalog` Spark session catalog**: configured via Glue Job
  parameters (`--datalake-formats=iceberg` plus `--conf` catalog settings)
  rather than a Databricks-managed catalog — this is the direct AWS
  equivalent of a metastore-backed Spark catalog.
- **Job Bookmarks for Bronze, a custom control table for Silver**: Bronze
  ingestion is naturally file-level incremental, which Job Bookmarks handle
  natively. Silver's unit of work is a batch (a `YYYY-MM` key spanning
  potentially many files and driving SCD logic), which bookmarks do not
  model — hence `silver_batch_control`.
- **DDL kept out of the orchestrated pipeline**: `create_tables.py` runs
  standalone; recurring data loads and schema management are different
  operational concerns and are kept decoupled.
- **Lake Formation Data Filters over SQL views**: row and column security
  are declared once at the grant level and apply uniformly across any
  query engine that respects Lake Formation (Athena, Glue, etc.), instead
  of being reimplemented per view.

## References

- Companion project (same business rule, Azure Databricks + Delta Lake):
  [github.com/cleberzumba/medallion-scd-pyspark](https://github.com/cleberzumba/medallion-scd-pyspark)
- [Apache Iceberg documentation](https://iceberg.apache.org/)
- [AWS Glue + Iceberg integration](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html)
- [AWS Lake Formation documentation](https://docs.aws.amazon.com/lake-formation/)
