# Lake Formation Governance — Setup Notes

This document records how data governance was configured for this project
using AWS Lake Formation, and one important pitfall that was found and fixed
along the way.

## 1. Databases and locations

Three databases were created in the Glue Data Catalog through Lake Formation,
each pointing at its own S3 prefix:

| Database          | Location                                                |
|--------------------|----------------------------------------------------------|
| `medallion_bronze` | `s3://medallion-scd-lakehouse-czs/warehouse/bronze/`      |
| `medallion_silver` | `s3://medallion-scd-lakehouse-czs/warehouse/silver/`      |
| `medallion_gold`   | `s3://medallion-scd-lakehouse-czs/warehouse/gold/`        |

## 2. Registering the S3 location

Lake Formation needs a data-access role to read/write the registered S3
location. The role selector in the "Register location" screen requires a
role whose trust policy allows `lakeformation.amazonaws.com` to assume it.
`medallion-glue-role` only trusts `glue.amazonaws.com`, so it does not appear
in that dropdown. Rather than adding a second trust relationship to the Glue
role, the location was registered with AWS's own service-linked role,
**`AWSServiceRoleForLakeFormationDataAccess`** — this is the AWS-recommended
default, not a workaround.

## 3. Principals

| Principal            | Type          | Purpose                                             |
|-----------------------|---------------|------------------------------------------------------|
| `cleber-admin`         | IAM user      | Human administrator, full access for setup/operations |
| `medallion-glue-role`  | IAM role      | Assumed by all Glue Jobs (create/read/write tables)   |
| `cleber-analyst`       | IAM user      | Read-only analyst, restricted via Lake Formation      |

## 4. Grants

- `cleber-admin` and `medallion-glue-role`: `Super` on all three databases
  (administrative access, table creation).
- `cleber-analyst`: `Describe` + `Select` only, scoped to `medallion_silver`
  and `medallion_gold`. No access to `medallion_bronze` (raw data should
  never be queried directly by analysts).

## 5. Data Filters (row and column level security)

Two data filters were created and applied to `cleber-analyst`'s grants:

- **Row filter** on `silver_dim_customers_type2` and
  `silver_dim_products_type2`: `is_current = true` — the analyst only ever
  sees the current version of each dimension row, never historical
  versions.
- **Column filter** (Exclude columns) on `silver_dim_customers_type2`:
  hides internal/history-tracking columns not relevant to business
  consumption, while keeping the row-level `is_current` filter in place.

This is Lake Formation's functional equivalent of Databricks Unity
Catalog's dynamic views / column masking, applied declaratively instead of
through a SQL view.

## 6. `cleber-analyst` IAM policy for Athena

`AmazonAthenaFullAccess` alone was not enough: it only grants S3 write
access to buckets matching `aws-athena-query-results-*`, and this project
uses a custom results bucket/prefix. A second, narrowly scoped managed
policy, **`AthenaResultsAccess`**, was attached:

- `s3:GetBucketLocation`, `s3:ListBucket` — bucket-level, restricted to the
  `athena-results/*` prefix via an S3 condition.
- `s3:GetObject`, `s3:PutObject` — object-level, restricted to
  `arn:aws:s3:::medallion-scd-lakehouse-czs/athena-results/*` only.

Deliberately, **no broader S3 permissions were granted**. All access to the
actual data in `warehouse/` is governed exclusively through Lake Formation,
not through IAM/S3 policy — this is the whole point of using Lake Formation
instead of relying on bucket policies alone.

## 7. Gotcha: `IAMAllowedPrincipals` legacy grants

While validating that `cleber-analyst` was correctly restricted, a serious
governance gap was found: Lake Formation's legacy backward-compatibility
group, **`IAMAllowedPrincipals`**, had blanket `Super` / `All` grants on all
three databases and every table in them (16 grants total). This group is
automatically granted access by Lake Formation for tables created the
"classic" (pre-Lake Formation) way, and it can persist unnoticed after
switching a workload over to fine-grained permissions.

The practical effect: **any IAM principal with sufficient raw IAM Glue/S3
permissions bypassed Lake Formation's fine-grained model entirely**,
because it matched `IAMAllowedPrincipals` before Lake Formation's own
per-principal grants were ever evaluated. `cleber-analyst`, via
`AmazonAthenaFullAccess`, had exactly this kind of raw IAM access — so
despite the row/column filters configured above, the analyst could still
see raw Bronze data and every column/row of Silver and Gold.

**Fix:** Lake Formation → Data permissions → filter by Principal =
`IAMAllowedPrincipals` → select all matching grants → **Revoke**.

**Verification after the fix:**

- `medallion_bronze` no longer appears at all in the database dropdown for
  `cleber-analyst` in the Athena query editor (a stronger signal than a
  query-time permission error — the database itself becomes invisible).
- Queries against `medallion_silver`/`medallion_gold` correctly return only
  the current row per key and correctly exclude the filtered columns.
- `cleber-admin` and `medallion-glue-role` were unaffected, since both
  already had their own explicit Lake Formation grants independent of
  `IAMAllowedPrincipals`.

**Takeaway:** enabling Lake Formation fine-grained permissions does not, by
itself, disable the legacy IAM-based access path. The `IAMAllowedPrincipals`
grants must be explicitly reviewed and revoked, or fine-grained governance
is silently bypassed for any principal with adequate IAM permissions.
