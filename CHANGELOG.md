# Changelog

All notable changes to the **Cartographer** analytics engine will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.4.0] - 2026-10-10

### Added
- **Analytics Consumption & Operationalization (Phase 4)**:
  - Materialized 2 reporting views in `models/marts/reporting/`: `rpt_seller_health_scorecard` and `rpt_executive_kpis_monthly` with zero physical storage redundancy.
  - Streamlit marketplace supply intelligence application (`app/dashboard.py` and `app/connection.py`) with 3 interactive drilldown views.
  - Least-privilege role consumption under role `REPORTER` with `USE SECONDARY ROLES NONE`.
  - Native Apache Arrow binary streaming (`fetch_pandas_all()`) and 10-minute caching (`@st.cache_data(ttl=600)`).
- **Continuous Integration & Dialect Linting**:
  - Snowflake dialect SQLFluff configuration (`.sqlfluff`, `.sqlfluffignore`) with 100% clean linting across all 22 models.
  - 3-stage GitHub Actions CI pipeline (`.github/workflows/ci.yml`) enforcing static linting, dbt compilation, and fast targeted marts/unit testing (48 tests in 13.17s).
- **Production Packaging & Governance**:
  - Authored comprehensive root [`README.md`](README.md) with Modern Data Stack diagram, engineering metrics, Kimball tri-fact layout, and quickstart.
  - Authored operational runbook [`docs/RUNBOOK.md`](docs/RUNBOOK.md) and expanded [`INTERVIEW_NOTES.md`](INTERVIEW_NOTES.md) with 5 system defense deep dives.
  - Built FinOps audit script (`scripts/audit_cost_governance.py`) asserting X-Small warehouse auto-suspend (60s), active resource monitor `CARTOGRAPHER_RM` (10 credit quota), and low credit burn (0.7568 credits).
  - Built documentation server (`scripts/serve_docs.py`) serving packaged static DAG lineage on port 8080.
  - Verified pipeline idempotency (zero row drift across all 3 Kimball facts on rerun).
  - Formal Architectural Decision Records ADR-015 through ADR-017 recorded and signed off in `DECISIONS.md`.

---

## [0.3.0] - 2026-10-09

### Added
- **Conformed Dimensions & Core Facts (Phase 3)**:
  - `dim_date`: Daily calendar dimension spanning 2016-09-01 to 2018-12-31 (852 rows).
  - `dim_sellers`: Conformed seller dimension capturing location and tenure (3,095 rows).
  - `dim_products`: Conformed product dimension with category translations and volumetric dimensions (32,951 rows).
  - `dim_customers`: Master customer dimension resolved at `customer_unique_id` grain with order history metrics (96,096 rows).
  - `fct_order_items`: Line-item transaction fact capturing item pricing without fan-out (112,650 rows, R$ 13,591,643.70 GMV).
  - `fct_seller_orders`: Accumulating snapshot fact tracking fulfillment lags, SLA compliance, and review scores at seller-order grain (100,010 rows).
  - `fct_seller_monthly_performance`: Periodic snapshot fact tracking monthly retention, churn transitions, fulfillment SLAs, and executive health tiers over a dense 33,484 seller-month spine.
- **Testing & Business Logic Verification**:
  - `tests/assert_seller_lifecycle_transitions.sql`: Singular unit test verifying churn state machine transitions (`new`, `active`, `at_risk`, `churned`, `dormant`, `reactivated`) and right-censoring behavior.
  - `tests/assert_fulfillment_sla_boundary_conditions.sql`: Singular unit test covering on-time handoffs, late dispatches, overdue unshipped orders, and impossible sequence exclusions.
  - `tests/assert_marts_reconciliation.sql`: Comprehensive Kimball invariant audit verifying financial parity (R$ 13,591,643.70), seller-order grain integrity (100,010), and dense monthly spine completeness (33,484).
  - Full suite verification: 61/61 tests PASS (generic schema + singular business tests).
- **Governance & Downstream Integration**:
  - `models/exposures.yml`: Added exposure for the Supply Operations Seller-Health Scorecard.
  - `DATA_DICTIONARY.md`: Authored single-source-of-truth enterprise data dictionary with grains, column types, and KPI references to `PROJECT_CONTEXT.md` §4.
  - Formal Architectural Decision Records ADR-007 through ADR-014 recorded in `DECISIONS.md`.
  - Failure Modes FM-006 through FM-009 documented in `FAILURE_MODES.md`.

---

## [0.2.0] - 2026-10-09

### Added
- **dbt Foundation & Staging Layer (Phase 2)**:
  - Materialized 9 deduplicated staging views in `models/staging/` (`stg_orders`, `stg_order_items`, `stg_order_reviews`, `stg_order_payments`, `stg_customers`, `stg_sellers`, `stg_products`, `stg_product_translations`, `stg_geolocation`) using windowed `QUALIFY ROW_NUMBER() = 1`.
  - Built 4 intermediate transformation models in `models/intermediate/`:
    - `int_reviews_rolled_up_to_orders`: Resolves 1:N fan-out by aggregating 99,224 reviews to 98,673 unique orders with worst-review arbitration.
    - `int_order_items_sellers_joined`: Enforces 1:1 item economics reconciliation.
    - `int_seller_orders_aggregated`: Aggregates line items to 100,010 seller-orders.
    - `int_seller_month_spine`: Dense Cartesian spine from seller first active month through 2018-08 (33,484 rows).
  - Added primary key uniqueness and non-null constraints across all staging and intermediate entities.

---

## [0.1.0] - 2026-10-09

### Added
- **PySpark Extraction & Contract Boundary (Phase 1, Part 1B)**:
  - Pinned runtime toolchain using Python 3.11, Java 17 LTS, and Apache Spark 3.5.x with deterministic loopback binding (`127.0.0.1`).
  - Frozen source baseline audit (`pyspark_ingestion/source_baseline.json`) capturing exact row counts (1,550,922 rows across 9 entities) and financial totals (R$13,591,643.70 price, R$2,251,909.54 freight, R$16,008,872.12 payments).
  - Explicit `StructType` schema definitions (`pyspark_ingestion/schemas.py`) enforcing `StringType` for 5-digit Brazilian postal codes (`zip_code_prefix`) and `DecimalType(12, 2)` for monetary values.
  - Fail-stop extraction engine (`pyspark_ingestion/extract.py`) utilizing `mode="FAILFAST"`, UniVocity `multiLine=true` (preserving 3,852 multiline CRLF comments across 99,224 logical reviews), and UTF-8 Byte Order Mark (BOM) sanitization.
  - Temporal replay engine (`pyspark_ingestion/replay_generator.py`) generating 22 monthly as-of partitions (`2017-01` to `2018-10`) with point-in-time timestamp masking, simulated late-arriving reviews (up to 528 days post-purchase), and lifecycle status regressions.
  - Deterministic Snappy Parquet serialization with `.coalesce(1)` per partition, reducing raw storage footprint from 122 MB to 60 MB across 93 files in `data/parquet/`.
  - Automated test suite (`tests/test_extract.py`) validating leading-zero preservation, BOM sanitization, multiline review parsing, decimal precision, fail-fast behavior, and status regressions (6/6 passing).
  - Automated conversion reconciliation report (`pyspark_ingestion/conversion_reconciliation.json`) confirming 100% row count and 0.00 financial drift parity.

- **Snowflake Infrastructure & Ingestion Scaffolding (Phase 1, Part 1C)**:
  - Database and Medallion architecture DDL (`snowflake/01_setup_database.sql`) defining `CARTOGRAPHER_DB` with `RAW`, `STAGING`, and `MARTS` schemas.
  - Role-Based Access Control (RBAC) matrix defining `LOADER`, `TRANSFORMER`, and `REPORTER` roles with `FUTURE TABLES` privilege cascading.
  - FinOps compute governance DDL (`snowflake/02_setup_warehouse.sql`) provisioning an X-Small virtual warehouse (`CARTOGRAPHER_WH`) with 60-second auto-suspend, 1800-second statement timeout, and a 10-credit monthly quota resource monitor (`CARTOGRAPHER_RM`).
  - Internal named stage DDL (`snowflake/03_storage_stage.sql`) creating `@OLIST_INTERNAL_STAGE` bound to `PARQUET_FORMAT` (`COMPRESSION = SNAPPY`).
  - Strongly-typed RAW table DDL and ingestion scripts (`snowflake/04_copy_ingestion.sql`) utilizing `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE` and `ON_ERROR = ABORT_STATEMENT`.

- **Documentation & Architectural Governance**:
  - Formal Architectural Decision Records ADR-001 through ADR-006 (`DECISIONS.md`).
  - Real-world failure modes and mitigation playbook (`FAILURE_MODES.md`).
  - Technical interview defense notes (`INTERVIEW_NOTES.md`).
  - Live recovery context ledger (`Resume context.md`).
