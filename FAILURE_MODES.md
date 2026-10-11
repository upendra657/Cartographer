# Real-World Failure Modes & Mitigation Playbook

This document details the operational edge cases, data corruptions, and infrastructure risks identified during Phase 1 ingestion, alongside their detection criteria, engineering resolutions, and architectural prevention mechanisms.

---

### FM-001: Multi-Line CRLF Line Breaks in Review Free-Text

* **Failure Scenario**: 
  The raw source file `Datasets/olist_order_reviews_dataset.csv` contains 99,224 logical review records. However, customer feedback in `review_comment_message` contains 3,852 raw multiline comments with unescaped Carriage Return and Line Feed (`\r\n` and `\n`) characters. A standard line-based CSV reader (`wc -l` reports 103,076 physical lines) splits single reviews across multiple records, generating fragmented records, shifting delimiters, and triggering downstream column misalignment.
* **Detection**:
  * Row count divergence: Standard streaming reader counts 103,076 rows instead of 99,224.
  * Parser failure: PySpark under `mode="FAILFAST"` immediately aborts with `MalformedCSVException` due to column count mismatches when encountering mid-comment newline characters.
* **Resolution**:
  * Configured Apache Spark's UniVocity parser with `.option("multiLine", "true")` and explicit `StructType` schemas in `pyspark_ingestion/extract.py`.
  * Verified extraction row count equals exactly 99,224 logical reviews with 100% field alignment.
* **Prevention**:
  * Pre-warehouse serialization converts unstructured CSV text into binary, columnar Snappy Parquet. In Parquet, variable-length string columns are serialized as length-prefixed byte arrays with zero dependence on newline or comma delimiters, immunizing Snowflake `COPY INTO` and dbt transformations against delimiter collision.
  * Regression prevented via automated unit test `test_multiline_review_parsing` in `tests/test_extract.py`.

---

### FM-002: UTF-8 Byte Order Mark (BOM) Header Contamination

* **Failure Scenario**:
  The reference mapping file `Datasets/product_category_name_translation.csv` (71 rows) was authored on Windows and saved with a 3-byte UTF-8 Byte Order Mark (`\xef\xbb\xbf` or Unicode `\ufeff`) prepended to the first column name.
* **Detection**:
  * Column name corruption: Inspecting the schema reveals `\ufeffproduct_category_name` rather than `product_category_name`.
  * Schema contract failure: Under PySpark `mode="FAILFAST"`, reading against an explicit schema expecting `product_category_name` throws an `AnalysisException: CSV header does not match schema`.
* **Resolution**:
  * Implemented header sanitization in `pyspark_ingestion/extract.py`: detects the leading `\ufeff` BOM signature on raw stream ingress and strips it before passing the schema to the Catalyst compiler.
* **Prevention**:
  * Explicit `StructType` contract in `pyspark_ingestion/schemas.py` rejects un-sanitized headers.
  * Validated via automated unit test `test_bom_sanitization` in `tests/test_extract.py`.

---

### FM-003: Brazilian Zip Code (CEP) Truncation & Leading Zero Loss

* **Failure Scenario**:
  Brazilian Postal Codes (Código de Endereçamento Postal — CEP) are 8-digit codes represented in the Olist dataset by their 5-digit prefix (`geolocation_zip_code_prefix`, `customer_zip_code_prefix`, `seller_zip_code_prefix`). Exactly **33.2% of seller zip codes** (1,028 of 3,095 sellers) and **24.1% of customer zip codes** (23,991 of 99,441 customers) originate in São Paulo state and begin with leading zeros (e.g. `01037`). Default schema inference (`inferSchema=true`) parses these numeric characters as integers, truncating `01037` to `1037` (4 digits).
* **Detection**:
  * Length check: `SELECT length(zip_code_prefix) FROM table WHERE length(zip_code_prefix) < 5` returns thousands of records.
  * Join failure: Geolocation lookups between `stg_olist__sellers` and `stg_olist__geolocation` on `zip_code_prefix` produce null spatial coordinates for 100% of leading-zero locations.
* **Resolution**:
  * Explicitly typed all zip code columns as `StringType` in PySpark (`pyspark_ingestion/schemas.py`).
  * Explicitly declared `VARCHAR(10)` in Snowflake RAW DDL (`snowflake/04_copy_ingestion.sql`).
* **Prevention**:
  * Strict prohibition against `inferSchema=true` in ingestion pipelines.
  * Automated regression test `test_leading_zero_preservation` in `tests/test_extract.py` asserts that seller `01037` retains its full 5-character string representation.

---

### FM-004: IEEE 754 Floating-Point Drift in Financial Columns

* **Failure Scenario**:
  Across 112,650 order items and 103,886 payment records, the marketplace processes R$13,591,643.70 in item value, R$2,251,909.54 in freight, and R$16,008,872.12 in customer payments. Storing currency using binary floating-point representations (`float` / `double` / `FLOAT64`) introduces binary fraction rounding artifacts (e.g. `0.1 + 0.2 = 0.30000000000000004`). In aggregate reporting, this produces cents-level rounding drift between total order item value and customer payments.
* **Detection**:
  * Financial reconciliation check: `SUM(price) + SUM(freight_value)` diverges from expected monetary sums by non-zero floating-point fractions (e.g. `0.0000000042`).
* **Resolution**:
  * Enforced fixed-point decimal arithmetic: `DecimalType(12, 2)` in PySpark (`pyspark_ingestion/schemas.py`) and `NUMBER(12, 2)` in Snowflake RAW DDL (`snowflake/04_copy_ingestion.sql`).
* **Prevention**:
  * Automated reconciliation in `pyspark_ingestion/conversion_reconciliation.json` verified 0.00 drift across all 1.55M records.
  * Automated unit test `test_monetary_precision` in `tests/test_extract.py` validates fixed-point decimal representation.
  * dbt data test suite enforces financial balance assertion: `payment_value == price + freight_value` within legitimate operational tolerance.

---

### FM-005: Snowflake Virtual Warehouse Credit Runaway & Cost Overruns

* **Failure Scenario**:
  Default Snowflake virtual warehouse configurations use 10-minute (600-second) auto-suspend times, lack statement execution timeouts, and have no account-level resource monitors attached. In development and CI/CD environments, ad-hoc queries, unoptimized cross-joins, or automated orchestration jobs can leave warehouses running continuously, consuming credits while idling.
* **Detection**:
  * Monitoring `SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY` shows warehouse uptime persisting long after query execution completes.
  * Cost telemetry flags compute spend exceeding project allocation budgets.
* **Resolution**:
  * Sized compute to **X-Small** (`CARTOGRAPHER_WH`, 1 credit/hour), perfectly matched to our 60 MB Parquet volume.
  * Configured aggressive auto-suspend: `AUTO_SUSPEND = 60` (shuts down after 60 seconds of inactivity).
  * Enforced execution guardrail: `STATEMENT_TIMEOUT_IN_SECONDS = 1800` (kills runaway queries after 30 minutes).
  * Created and assigned a monthly 10-credit Resource Monitor (`CARTOGRAPHER_RM`):
    * 75% quota: Notify administrators.
    * 90% quota: Notify administrators.
    * 100% quota: Hard suspend warehouse immediately.
* **Prevention**:
  * All warehouse parameters provisioned declaratively via version-controlled DDL in `snowflake/02_setup_warehouse.sql`.
  * RBAC architecture (`LOADER`, `TRANSFORMER`, `REPORTER`) ensures individual users cannot alter warehouse auto-suspend or quota configurations.

---

### FM-006: Order-to-Review Fan-Out (+5.02% GMV Inflation)

* **Failure Scenario**:
  Orders join 1:N to customer reviews. Exactly 547 orders received 2 or 3 reviews (e.g. initial complaint followed by post-resolution re-review). A naive tabular join `orders LEFT JOIN reviews ON orders.order_id = reviews.order_id` duplicates line items on those 547 orders, falsely inflating Gross Merchandise Value by +R$ 682,300 (+5.02%) across downstream dashboards.
* **Detection**:
  * Financial reconciliation assertion: `SUM(fct_order_items.price)` exceeds baseline R$ 13,591,643.70.
  * Lineage audit: `fct_order_items` row count expands beyond 112,650 items.
* **Resolution**:
  * Implemented `int_reviews_rolled_up_to_orders` to aggregate 99,224 raw reviews to the 98,673 unique `order_id` grain prior to any dimensional joins.
  * Applied conservative worst-review-score arbitration (`MIN(review_score)`) and latest comment text extraction.
* **Prevention**:
  * Strict prohibition against joining un-aggregated review logs to order-grain facts.
  * Singular reconciliation test `assert_marts_reconciliation.sql` asserts GMV matches raw baseline to R$ 0.01.

---

### FM-007: Late-Arriving Reviews & Lifecycle Events Beyond Incremental Lookback

* **Failure Scenario**:
  Customer reviews arrive up to 528 days post-purchase (e.g., warranty claims and long-tail disputes). An incremental dbt run with an append-only strategy or an overly narrow lookback window misses these updates, leaving fulfillment SLA, ODR, and seller health scores permanently frozen with incomplete data.
* **Detection**:
  * Comparing incremental run outputs against a full-refresh build reveals drift on historical seller-order rows.
* **Resolution**:
  * Materialized `fct_seller_orders` as an incremental model with `incremental_strategy = 'merge'` and a 2-month lookback window (`order_purchase_timestamp >= DATEADD('month', -2, current_max)`), capturing 99.52% of all lifecycle updates.
  * Tagged every monthly snapshot with an `is_final` flag (TRUE only after month close + 2 full calendar months).
  * Governed the remaining 473 long-tail orders (0.48%) via scheduled monthly full-refresh builds.
* **Prevention**:
  * Automated drift monitoring comparing incremental vs full-refresh builds (ADR-012, ADR-013).

---

### FM-008: Downstream Lifecycle Status Regressions (Canceled After Shipped)

* **Failure Scenario**:
  Orders experience retroactive status regressions (e.g., 6 orders shipped in earlier months were retroactively marked `canceled` due to transit carrier loss or fraud chargebacks). If downstream fact tables do not re-evaluate past months, seller GMV remains overstated and fulfillment metrics miscount non-billable revenue.
* **Detection**:
  * Difference in canceled order counts between batch ingress watermarks.
* **Resolution**:
  * `fct_seller_monthly_performance` recalculates GMV and freight using `NOT is_canceled AND NOT is_unavailable` filters over the lookback window.
  * Deduplicated staging layer ensures the latest `order_status` wins via `QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _extracted_at DESC) = 1`.
* **Prevention**:
  * Incremental merge lookback re-evaluates all open and recently closed monthly partitions upon status changes.

---

### FM-009: Dense Calendar Spine Combinatorial Explosion at Scale

* **Failure Scenario**:
  Constructing a dense seller-month spine via an unconstrained Cartesian cross-join (`sellers CROSS JOIN all_months`) generates millions of artificial rows for merchants before they ever registered or made a sale, diluting churn rate calculations with phantom inactive months and bloating warehouse memory.
* **Detection**:
  * Spine row count scales as `N_sellers * total_calendar_months`, resulting in unexpected query execution times and memory spills.
* **Resolution**:
  * Implemented `int_seller_month_spine` with a left-bounded date filter: each seller's spine starts strictly on their `first_active_month` (`d.month >= s.first_active_month`).
  * Right-bounded at analysis cutoff (`2018-08-01`), accurately right-censoring active merchants and preventing phantom churn.
* **Prevention**:
  * Verified exact spine row count (33,484 rows) via singular test `assert_marts_reconciliation.sql`.

