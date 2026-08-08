# E-Commerce Sales Analytics Pipeline

An end-to-end data engineering project: MySQL (OLTP source) → S3 (raw landing
zone) → Databricks (bronze/silver/gold, Delta Lake) with a Slowly Changing
Dimension Type 2 for customer history, unit-tested transformation logic, and
Git-based CI/CD.

## Architecture

```
MySQL (RDS)  --JDBC-->  S3 raw/  --Databricks-->  Delta bronze  -->  Delta silver  -->  Delta gold
                                                                                          (SCD2 dims + facts)
```

- **Bronze**: raw data landed as-is from S3, append-only, metadata columns added.
- **Silver**: cleaned, deduplicated, standardized, invalid records quarantined
  (not dropped).
- **Gold**: `dim_customer` (SCD Type 2), `dim_product`, `fact_sales`.

## Repository layout

```
sql/                    MySQL DDL for the source OLTP schema
ingestion/              Faker-based seed data generator + MySQL -> S3 extractor
transformations/        bronze.py, silver.py, gold_scd2.py (pure functions + I/O)
tests/                  pytest unit tests (local PySpark session, no cluster needed)
configs/config.yaml     Central pipeline config (non-secret)
.github/workflows/      CI: lint + unit tests on PR, deploy to Databricks on merge
```

## Slowly Changing Dimension Type 2 — `dim_customer`

Implemented in `transformations/gold_scd2.py`. Tracks history on:
`loyalty_tier`, `address`, `city`, `state`, `postal_code`, `email`.

Each row in `dim_customer` represents one version of a customer:

| column | purpose |
|---|---|
| `customer_sk` | surrogate key, unique per version |
| `customer_id` | natural/business key from MySQL |
| `row_hash` | SHA-256 over tracked columns, used to detect real changes |
| `effective_start_date` / `effective_end_date` | validity window |
| `is_current` | `True` for the active version |

The core logic is split into pure, unit-testable functions:
- `add_row_hash` — change detection fingerprint
- `initialize_dimension` — bootstrap on first run
- `compute_scd2_snapshot` — the actual SCD2 merge logic (expire changed rows,
  insert new versions, insert new customers, carry forward unchanged/historical rows)

I/O (reading `silver.customers`, writing the Delta table) is isolated in
`run_dim_customer_scd2()`, kept separate from the logic so tests don't need a
live cluster.

**Design decisions worth knowing:**
- Only *tracked* columns trigger a new version. A typo fix to `first_name`,
  for example, updates in place rather than creating pipeline history noise.
- Customers missing from a given day's extract are **not** auto-expired —
  that would silently rewrite history on a transient extraction gap. Hard
  deletes need an explicit `is_deleted` flag from the source.
- The current implementation recomputes the full dimension each run (simple,
  fully testable). At larger scale this maps directly onto a Delta
  `MERGE INTO ... WHEN MATCHED / WHEN NOT MATCHED` statement instead of a
  full rebuild — same logic, different execution strategy.

## Unit tests

```bash
pip install -r requirements.txt
pytest tests/ -v
```

19 tests covering:
- Silver-layer cleaning (dedup, casing, invalid-tier defaults, email
  validation flags, quarantine of bad rows, discount math)
- SCD2 logic (hash stability, change detection, versioning, new customers,
  missing-from-source handling, multi-generation history)

Tests run against a local PySpark session — no Databricks cluster or AWS
credentials required.

## Running the pipeline

1. **Seed source data**: `python ingestion/generate_seed_data.py --host <rds-endpoint> --user ... --password ...`
2. **Extract to S3**: run `ingestion/extract_mysql_to_s3.py` as a Databricks job/task
3. **Bronze**: `transformations/bronze.py <run_date>`
4. **Silver**: `transformations/silver.py`
5. **Gold**: `transformations/gold_scd2.py` (builds `dim_customer` + `fact_sales`)

In production these are chained as a Databricks Workflow (bronze → silver →
gold) on a daily schedule, defined in `configs/config.yaml`.

## CI/CD

`.github/workflows/ci.yml` runs flake8 + the full pytest suite on every PR,
and syncs the repo into a Databricks workspace via Databricks CLI on merge
to `main`.

## Stretch goals

- Structured Streaming ingestion (Kinesis → Databricks) for near-real-time orders
- Delta Live Tables expectations for declarative data quality
- `dim_product` versioning (currently type-1, could extend to SCD2 for price history)
- Databricks SQL dashboard on top of `fact_sales`
