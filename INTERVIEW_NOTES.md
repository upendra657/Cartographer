# Cartographer: Technical Interview Notes & Defense Strategies

This document serves as the repository of architectural defense rationales, line-by-line script explanations, and high-stakes interview talking points for the **Cartographer** marketplace supply analytics engine.

---

## Phase 1: PySpark ELT and Snowflake Ingestion

### Step 1.2: Local Spark Toolchain & Runtime Architecture

#### 1. Why Java 17 LTS and Python 3.11?
* **Java 17 vs. Bleeding-Edge Java (19+)**:
  * Apache Spark 3.5.x compiles and runs strictly against Java 8, 11, and 17.
  * Starting in Java 9 and strictly enforced in Java 17+, the Java Platform Module System (JPMS) enforces strong encapsulation of internal JDK packages.
  * Running Spark on Java 19+ causes fatal JVM reflection errors during object serialization and off-heap memory management:
    `java.lang.reflect.InaccessibleObjectException: Unable to make field private final java.lang.Object[] java.util.ArrayList.elementData accessible`.
  * Spark 3.5 targets Java 17 LTS as its modern baseline, passing explicit JVM reflection opening arguments (`--add-opens=java.base/java.nio=ALL-UNNAMED`).
* **Python 3.11 vs. Bleeding-Edge Python (3.13/3.14)**:
  * Analytical ecosystem tools (`pyarrow`, `dbt-core 1.8`, `dbt-snowflake`, `snowflake-connector-python`) depend on pre-compiled C/Cython extensions.
  * Python 3.11 provides universal binary wheel coverage across macOS ARM64 and Linux x86_64, eliminating local compilation failures and C-API ABI breakages.

#### 2. Local SparkSession Initialization Options
```python
spark = (
    SparkSession.builder
    .master('local[1]')
    .appName('Cartographer-Toolchain-SmokeTest')
    .config('spark.driver.bindAddress', '127.0.0.1')
    .config('spark.ui.enabled', 'false')
    .getOrCreate()
)
```
* **`.master('local[1]')`**:
  * Executes Spark locally using exactly one worker thread.
  * **Interview Rationale**: Prevents multi-threaded concurrency overhead and thread contention on development laptops during lightweight smoke tests and deterministic unit tests, while still executing through Spark Catalyst and Tungsten engines.
* **`.config('spark.driver.bindAddress', '127.0.0.1')`**:
  * Forces the Spark driver to bind strictly to the loopback interface (`localhost`).
  * **Interview Rationale**: On macOS, Spark defaults to resolving the local hostname via reverse DNS. If local networking or VPN configurations delay DNS resolution, driver initialization hangs or triggers macOS firewall security prompts. Binding to `127.0.0.1` guarantees deterministic, zero-latency network binding.
* **`.config('spark.ui.enabled', 'false')`**:
  * Disables the embedded Spark Web UI (default port 4040).
  * **Interview Rationale**: In automated test suites and lightweight CLI execution, launching an embedded Jetty HTTP server incurs CPU and memory overhead and causes socket collision errors (`BindException: Address already in use`) when consecutive test sessions run concurrently.
* **`assert count == 3095`**:
  * Tests that the JVM, file reader, and Catalyst optimizer can successfully parse a real CSV without schema corruption, confirming against the known natural row count of `Datasets/olist_sellers_dataset.csv`.

---

### Step 1.3 & 1.4: Source Baseline Freezing & Explicit Type Contracts

#### 1. Why is Schema Inference (`inferSchema=true`) Forbidden in Production?
* **Dual Full Table Scans**:
  * Spark's `inferSchema=True` forces a full preliminary pass over the entire file to sample and deduce data types before executing the actual read job. For large datasets, this doubles disk I/O and network transfer times.
* **Finding F3: Silent Corruption of Brazilian CEP Zip Codes**:
  * 33.2% of seller zip codes and 24.1% of customer zip codes begin with leading zeros (e.g. `01037` in São Paulo).
  * Schema inference deduces numeric characters as `IntegerType`, stripping the leading zero and mutating `01037` to `1037`. Downstream spatial and entity-resolution joins to geolocation tables immediately fail.
  * **Defense**: CEP prefixes must be explicitly defined as `StringType`.
* **IEEE 754 Floating-Point Drift**:
  * Inferring monetary amounts (`price`, `freight_value`, `payment_value`) casts them to `DoubleType` (binary 64-bit float).
  * Because binary floating-point numbers cannot accurately represent fractional decimals like `0.1` or `0.01`, summing R$13.6M across 112,650 transaction rows leads to rounding errors and cents-level drift.
  * **Defense**: Monetary metrics are explicitly cast to `DecimalType(12, 2)` (fixed-point arithmetic), guaranteeing reconciliation to the exact cent.

#### 2. Raw Fidelity Rule (Step 1.6 Preview)
* **Preserving Source Typos (`product_name_lenght`, `product_description_lenght`)**:
  * In production ELT architecture, the ingestion layer must never rename columns or implement business logic.
  * The RAW layer in Snowflake must remain an immutable, 1:1 mirror of the source contract. Renaming `product_name_lenght` to `product_name_length` occurs exclusively in the dbt staging layer (`stg_olist__products.sql`). If upstream fixes their typo in the future, the ingestion contract remains unbroken.

---

### Step 1.5: Parser Configuration & UniVocity Multi-Line Mechanics

#### 1. Handling CRLF Line Breaks in Customer Reviews (Finding F1)
* **The Symptom**: `Datasets/olist_order_reviews_dataset.csv` contains **104,720 physical lines**, but represents exactly **99,224 logical records**.
* **The Root Cause**: 3,852 Portuguese customer reviews contain embedded carriage returns and line feeds (`\r\n` and `\n`) inside user-submitted comments (`review_comment_message`).
* **The Distributed Failure Mode**:
  * Apache Spark's default CSV reader splits input files into 32 MB or 64 MB byte splits across distributed worker tasks. By default (`multiLine=false`), it assumes every newline character marks an atomic record delimiter.
  * When a newline occurs inside a quoted string, default Spark splits the record in half. The second half of the review comment is treated as a new row, shifting all columns (e.g., placing Portuguese text into a timestamp field). This triggers silent NULL generation or parser crashes.
* **The Production Resolution**:
  * `.option("multiLine", "true")`: Configures Spark's underlying UniVocity parser to maintain state across newlines inside quoted strings (`"..."`).
  * **Interview Trade-Off & Defense**: Enabling `multiLine=true` disables parallel byte-chunk splitting for that file—the file is streamed through a single partition/core. At 14.5 MB, single-core processing takes < 1 second and provides a mathematical guarantee against row fragmentation and column shifting.
  * `.option("escape", "\"")`: Ensures escaped quotes (`""`) inside review titles and comments do not prematurely close quoted string boundaries.

#### 2. Handling the Windows UTF-8 Byte Order Mark (BOM) (Finding F2)
* **The Symptom**: In `Datasets/product_category_name_translation.csv`, the first column header reads as `\ufeffproduct_category_name` instead of `product_category_name`.
* **The Root Cause**: The source CSV was exported with a Windows UTF-8 Byte Order Mark (BOM: `0xEF, 0xBB, 0xBF`).
* **The Failure Mode**: While invisible in text editors, the leading zero-width non-breaking space character (`\ufeff`) becomes part of the column name string in Spark. Downstream dbt staging models querying `SELECT product_category_name` will fail with `column not found`.
* **The Production Resolution**:
  * Set `.option("encoding", "UTF-8")`.
  * In `extract.py`, inspect `df.columns[0]` and programmatically strip `\ufeff` with `withColumnRenamed()` if present, guaranteeing exact downstream contract conformity.

#### 3. Why `FAILFAST` Mode is Mandatory (ADR-002)
* **Spark Parse Modes Compared**:
  * `PERMISSIVE` (Default): Replaces malformed fields with `NULL` and places the raw corrupt string in a hidden `_corrupt_record` column.
  * `DROPMALFORMED`: Silently deletes corrupted rows from the output DataFrame.
  * `FAILFAST` (Cartographer Standard): Immediately aborts the job and throws a `SparkException` upon encountering the first malformed token or unparseable datatype.
* **Interview Defense (ADR-002)**:
  * In a batch ELT ingestion engine, `PERMISSIVE` is dangerous because corrupted numbers silently become `NULL`, falsifying financial reconciliations (e.g., losing R$10,000 in GMV without triggering an alert).
  * `DROPMALFORMED` is strictly forbidden because it introduces silent data loss and breaks referential integrity (creating orphan order items).
  * `FAILFAST` forces data engineering accountability: if a row fails parsing, it is either an unhandled edge case or an invalid schema contract. The pipeline halts before corrupted records can ever land in Snowflake.

#### 4. Implementation Details: `extract_csv()` Configuration Walkthrough
```python
reader = (
    spark.read
    .format("csv")
    .schema(schema)
    .option("header", "true")
    .option("mode", "FAILFAST")
    .option("encoding", "UTF-8")
    .option("escape", "\"")
    .option("quote", "\"")
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
)

if table_name == "olist_order_reviews_dataset":
    reader = reader.option("multiLine", "true")

df = reader.load(file_path)

# Clean BOM artifact on header if present (Finding F2)
first_col = df.columns[0]
if first_col.startswith("\ufeff"):
    clean_first_col = first_col.replace("\ufeff", "")
    df = df.withColumnRenamed(first_col, clean_first_col)
```

* **Line-by-Line Parser Configuration Defense**:
  * `.schema(schema)`: Binds the declared `StructType` contract directly during reading, completely bypassing two-pass schema inference (`inferSchema=true`).
  * `.option("mode", "FAILFAST")`: Implements ADR-002. Enforces fail-stop semantics. If any row contains delimiter corruption, type mismatch, or quote imbalance, the UniVocity parser aborts the Spark job immediately with a `SparkException` rather than silently converting corrupted data into `NULL` (`PERMISSIVE`) or dropping rows (`DROPMALFORMED`).
  * `.option("encoding", "UTF-8")`: Ensures standard UTF-8 character decoding across Portuguese accents (e.g., São Paulo, cartão de crédito).
  * `.option("escape", "\"")` and `.option("quote", "\"")`: Complies with standard RFC 4180 CSV specifications where double quotes inside text fields are escaped with double quotes (`""`).
  * `.option("timestampFormat", "yyyy-MM-dd HH:mm:ss")`: Eliminates timezone parsing ambiguity across Olist timestamp columns.
  * `.option("multiLine", "true")` for `olist_order_reviews_dataset`: Activates UniVocity's multi-line state machine buffering. Prevents 3,852 Portuguese customer reviews with embedded CRLF (`\r\n`) and LF (`\n`) from fragmenting records across worker partitions, correctly parsing 99,224 logical records from 104,720 physical lines (Finding F1).
  * `first_col.startswith("\ufeff")` & `withColumnRenamed()`: Strips the Windows UTF-8 Byte Order Mark (`0xEF, 0xBB, 0xBF`) from `product_category_name_translation.csv`, ensuring the first column resolves cleanly as `product_category_name` rather than `\ufeffproduct_category_name` (Finding F2).

---

### Step 1.6: Raw Fidelity Rule & Technical Metadata Columns

#### 1. The Raw Fidelity Rule
* **Principle**: The ingestion layer performs **zero column renames, zero deduplication, and zero business logic**.
* **Handling Source Typos (`product_name_lenght`, `product_description_lenght`)**:
  * Inexperienced engineers often "clean" typos in their Python extraction scripts.
  * **Senior AE Defense**: If an ingestion script alters source column names, the RAW layer in the data warehouse is no longer an auditable contract mirror of the source system. If the upstream transactional database fixes the typo in a future migration, the ingestion script breaks.
  * In Cartographer, `product_name_lenght` lands in Snowflake RAW with the typo intact. All corrections and human-readable aliases occur strictly in dbt staging models (`stg_olist__products.sql`).

#### 2. Technical Audit Metadata Columns
To support governance, lineage tracking, and replay idempotency, the ingestion layer appends audit metadata:
* `_batch_id`: Identifies the monthly replay batch (e.g., `2017-01` through `2018-10`) to support incremental testing.
* `_as_of_date`: The logical point-in-time boundary of the extracted records.
* `_extracted_at`: UTC timestamp marking when the Spark extraction job executed.
*(Snowflake will subsequently append `_source_file` and `_loaded_at` during the `COPY INTO` stage).*

#### 3. Implementation Details: `add_technical_metadata()` Walkthrough
```python
def add_technical_metadata(
    df: DataFrame,
    batch_id: str = "2018-10",
    as_of_date: str = "2018-10-31"
) -> DataFrame:
    return (
        df
        .withColumn("_batch_id", lit(batch_id))
        .withColumn("_as_of_date", to_date(lit(as_of_date)))
        .withColumn("_extracted_at", current_timestamp())
    )
```

* **Line-by-Line Technical Defense**:
  * `_batch_id`: Injected as an explicit partition key (`StringType`). When written to Parquet, directory partitioning by `_batch_id` allows granular loading into Snowflake internal stages and deterministic incremental testing (Step 1.7 & 1.8).
  * `_as_of_date`: Cast as `DateType`. Represents the business cut-off boundary for the batch. In downstream mart models, late-arriving events are analyzed relative to this watermark.
  * `_extracted_at`: Injected via `current_timestamp()` in UTC (`spark.sql.session.timeZone = "UTC"`). Records the exact physical runtime of the extraction job for lineage and SLA auditability.

---

### Step 1.7: Parquet Output Layout & File Sizing Architecture

#### 1. Hive Directory Partitioning Pattern
* **Layout**: `data/parquet/<entity>/batch_id=<YYYY-MM>/part-*.snappy.parquet`
* **Why Partition Strictly by `_batch_id`?**:
  * **Avoiding the Small Files Problem**: Partitioning by high-cardinality dimensions (e.g., `order_date` or `seller_id`) would create thousands of microscopic sub-megabyte files. In distributed object storage (S3/GCS), every file requires a separate `LIST` and `GET` API call, severely degrading filesystem metadata throughput and Spark shuffle efficiency.
  * **1:1 Alignment with Warehouse Ingestion**: In Snowflake, `batch_id=<YYYY-MM>` directly maps to internal stage subpaths (`@stage/orders/batch_id=2017-01/`), enabling deterministic, partition-pruned `COPY INTO` execution.

#### 2. Compression & Target File Sizing
* **Snappy Compression**:
  * Balances compression ratio (~3x reduction, shrinking 120 MB CSV to ~35-40 MB Parquet) with ultra-high decompression throughput.
  * Unlike GZIP or BZIP2, Snappy is optimized for CPU-bound decompression speed during analytical scans.
* **1–4 Files per Entity per Batch**:
  * Snowflake documentation recommends loaded files be 100–250 MB compressed for optimal thread utilization on standard virtual warehouse workers.
  * At 120 MB total dataset size, a monthly batch represents 1–5 MB. We coalesce Spark output partitions (`coalesce(1)`) so each monthly batch produces exactly 1 clean Parquet file, completely eliminating file fragmentation.

---

### Step 1.8: As-Of Replay Generator Mechanics (Simulating Late-Arriving Data & Mutations)

#### 1. Why an As-Of Replay Generator? (ADR-003)
* **The Failure of Static Ingestion**:
  * Standard public projects load a static CSV dump into the warehouse once. This makes it impossible to validate real production capabilities: incremental `MERGE` logic, lookback windows, late-arriving events, and status regressions.
* **The Cartographer Replay Architecture**:
  * We generate **22 discrete monthly batches** (`2017-01` through `2018-10`).
  * `2017-01` (Baseline): Carries all historical data up to January 31, 2017 (including 2016 orders).
  * `2017-02` through `2018-08`: Regular monthly business operations.
  * `2018-09` through `2018-10`: Captures late review answers and trailing lifecycle completions.

#### 2. Simulating Late-Arriving Data & Mutations (Findings F9, F13)
* **Order Lifecycle Milestones**:
  * An order purchased in month $M$ appears in batch $M$.
  * Subsequent timestamps (`order_approved_at`, `order_delivered_carrier_date`, `order_delivered_customer_date`) are strictly masked to `NULL` if their actual timestamp is greater than the batch's `as_of_date`.
  * `order_status` is dynamically calculated based on the furthest milestone reached as of that date. When delivery occurs 2 months later, the order re-appears in that batch with an updated status (`shipped` $\rightarrow$ `delivered`), exercising dbt incremental updates.
* **Late-Arriving Reviews**:
  * Reviews are released when answered (`review_answer_timestamp <= as_of_date`).
  * **Finding F13**: Reviews arrive up to 528 days after purchase. The replay generator emits these reviews in their answer month, testing dbt's ability to join late reviews back to historical orders without fan-out.
  * **Finding F9 Constraint**: 74 reviews in Olist have creation dates prior to purchase timestamp. The replay generator enforces that no review can be released prior to its order's purchase batch.
* **Status Regression Edge Case**:
  * 6 orders in Olist carry delivery timestamps but have a final status of `canceled`.
  * The generator emits them as `delivered` when delivered, then regresses them to `canceled` in the next batch, validating whether the downstream pipeline handles status rollbacks.

---

### Step 1.9: Automated PySpark Hazard Testing (`tests/test_extract.py`)

#### 1. Why Test Small Synthetic Fixtures Instead of the Full Dataset?
* **Senior AE Defense**:
  * Running full integration tests on 1.5M rows for every commit or PR creates slow, expensive test suites.
  * Unit tests must isolate specific parsing failure modes using tiny, deterministic in-memory micro-fixtures:
    1. `test_leading_zero_cep_preservation`: Validates that `01037` remains `StringType` and doesn't get mutated to integer `1037`.
    2. `test_bom_header_sanitization`: Confirms `\ufeff` is stripped from column 0.
    3. `test_multiline_crlf_reviews_parsing`: Confirms UniVocity `multiLine=true` preserves internal `\r\n` without splitting records.
    4. `test_decimal_precision_no_float_drift`: Proves `DecimalType(12, 2)` produces exact arithmetic (`19.90 + 10.10 = 30.00`) with zero IEEE 754 drift.
    5. `test_failfast_mode_aborts_on_corrupt_record`: Proves `mode="FAILFAST"` immediately raises an exception on malformed tokens.
    6. `test_status_regression_logic`: Verifies that canceled orders with delivery dates correctly derive status transitions across replay batches.

#### 2. Resolving the macOS Python Version Mismatch in PySpark
* **The Failure**: When Spark tasks execute Python UDFs or create DataFrames, worker processes default to the system `python3` binary on `PATH` (macOS Homebrew Python 3.14), causing a fatal `PySparkRuntimeError: [PYTHON_VERSION_MISMATCH] (3, 14) vs driver 3.11`.
* **The Fix**: Pin worker environment variables explicitly to the virtual environment binary:
  ```python
  os.environ["PYSPARK_PYTHON"] = sys.executable
  os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
  ```

---

### Step 1.10: Conversion Reconciliation Ledger

#### 1. Zero Data Drift Guarantee
Before touching Snowflake, the Parquet replay outputs were verified against `source_baseline.json`:
* **Row Count Parity**: 1,550,922 / 1,550,922 rows (0 drift).
* **Financial Metric Parity**:
  * $\Sigma\text{price} = \text{R\$13,591,643.70}$ (0.00 drift)
  * $\Sigma\text{freight} = \text{R\$2,251,909.54}$ (0.00 drift)
  * $\Sigma\text{payment} = \text{R\$16,008,872.12}$ (0.00 drift)
* **Storage Footprint**: Raw CSV (122 MB) $\rightarrow$ Snappy Parquet (60 MB across 93 files in 22 monthly batches).

---

### Step 1.12: Snowflake RBAC & Medallion Security Architecture

#### 1. The Principle of Least Privilege in Modern Data Engineering
* **Enterprise Role Matrix**:
  * `LOADER`: Dedicated ingestion role. Has full privileges over `RAW`, but zero access to `STAGING` or `MARTS`. Guarantees an ingestion engine cannot corrupt business transform layers.
  * `TRANSFORMER` (dbt): Owns transformation. Has `SELECT` on `RAW` and full privileges over `STAGING` and `MARTS`. Critically, `TRANSFORMER` cannot modify or delete `RAW` tables, preserving the immutable load log (ADR-008).
  * `REPORTER`: Read-only consumer role for BI tools (Tableau/Metabase) and ad-hoc analysts. Limited strictly to `SELECT` on `MARTS`. Prevents end users from querying raw PII or uncurated intermediate data.
* **Why `FUTURE TABLES` Grants Are Mandatory**:
  * In standard Snowflake SQL, `GRANT SELECT ON ALL TABLES IN SCHEMA ...` only grants access to tables existing at that exact millisecond.
  * When dbt or PySpark creates a new table or executes `CREATE OR REPLACE`, standard permissions do not cascade. `GRANT ... ON FUTURE TABLES` guarantees newly materialized tables inherit correct permissions automatically without manual DBA intervention.

---

### Step 1.13: Compute Warehouse Sizing & Cost Governance (ADR-006)

#### 1. Why X-Small Warehouse?
* **Cost vs. Volume Trade-off**:
  * An X-Small warehouse consumes **1 credit/hour** (~$2.00–$3.00/hr) across a single 8-core virtual server.
  * At 120 MB total dataset size (60 MB compressed Parquet), data operations fit entirely in local SSD and memory caches. Sizing up to Small (2 credits/hr) or Medium (4 credits/hr) doubles costs with zero latency benefit because the bottleneck is network I/O, not compute power.
* **Aggressive Auto-Suspend (60 seconds)**:
  * Snowflake bills per-second with a 60-second minimum. Setting `AUTO_SUSPEND = 60` ensures the warehouse shuts down immediately after query completion, eliminating idle credit bleed.
* **Resource Monitor Hard Limits**:
  * `ON 100 PERCENT DO SUSPEND`: Prevents runaway queries, recursive joins, or unconstrained loops from exhausting monthly enterprise credits.

---

### Step 1.14: Snowflake Internal Stage & Storage Architecture (ADR-004)

#### 1. Why an Internal Named Stage over S3 External Stage?
* **Zero Cloud Credential Coupling**:
  * An external stage requires configuring an AWS IAM Role, STS trust policy, and Snowflake Storage Integration.
  * For local batch replay development, an internal named stage (`@OLIST_INTERNAL_STAGE`) allows direct uploading via SnowSQL / Python Connector `PUT` commands without cloud infrastructure overhead or cloud egress costs.
* **Declarative File Format Binding**:
  * Binding `FILE_FORMAT = PARQUET_FORMAT` (`COMPRESSION = SNAPPY`) directly to the stage guarantees that any `COPY INTO` or query against `@OLIST_INTERNAL_STAGE` interprets the binary column layout consistently.

---

### Step 1.15: Strongly-Typed RAW Ingestion Mechanics (ADR-002, ADR-005)

#### 1. Why `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`? (ADR-005)
* **Positional Fragility ($1, $2, $3) Rejected**:
  * Standard CSV-era `COPY INTO` scripts map columns positionally (`SELECT $1, $2, $3 FROM @stage`).
  * If upstream modifies column order in Parquet, positional loading silently loads timestamps into names or amounts into IDs.
  * `MATCH_BY_COLUMN_NAME` reads Parquet column metadata directly, binding strictly by identifier name and preventing positional misalignments.
* **Why Typed Tables over a Single VARIANT Column?**:
  * Storing everything in a single `VARIANT` column delays type validation to query time.
  * Explicitly typed DDL (`NUMBER(12, 2)`, `TIMESTAMP_NTZ`, `VARCHAR`) enforces contract conformity at load time: if a type cannot be coerced, the load fails immediately.

#### 2. The Fail-Stop Error Policy (`ON_ERROR = ABORT_STATEMENT`) (ADR-002)
* **Eliminating Silent Corruption**:
  * `ON_ERROR = CONTINUE` or `SKIP_FILE` permits corrupt rows to be discarded, breaking financial balances and referential integrity.
  * `ABORT_STATEMENT` guarantees atomic transactions: if a single record violates the contract, the entire batch rolls back, alerting the engineer before downstream marts are computed.

---

### Step 1.17: Phase 1 Architectural Decision Records & Failure Modes Defense

#### 1. Why Maintain Formal ADRs (ADR-001 through ADR-006)?
* **Institutional Memory**: Architecture drifts when team members rotate or when choices are made implicitly without recorded alternatives. ADRs document the operational context, evaluated alternatives, and specific trade-offs at the exact time of implementation.
* **Staff Engineer Standard**: In high-stakes system design interviews, distinguishing between "I chose X because it's popular" and "I chose X over Y and Z because volume (60 MB) made warehouse compute I/O-bound, rendering larger virtual warehouses wasteful" is the core benchmark of Staff-level maturity.

#### 2. Defending Key Ingestion Edge Cases (FM-001 through FM-005)
* **Multiline Reviews (FM-001)**:
  * "Why not just strip newlines in a bash script?"
  * *Defense*: Mutating raw text files with ad-hoc regex scripts destroys auditability and can strip legitimate semantic line breaks in customer feedback. Instead, we use Spark's UniVocity parser with `multiLine=true` to parse raw text with 100% fidelity, then serialize to binary Snappy Parquet where length-prefixed byte arrays eliminate delimiter ambiguity forever.
* **Brazilian Zip Codes (FM-003)**:
  * "Why treat numbers as strings?"
  * *Defense*: Numeric data types are for values intended for mathematical operations (sum, average). Postal codes, telephone numbers, and tax identifiers are categorical character tokens. In Brazil, 33.2% of seller CEPs begin with `0`. Casting to integer strips significant digits (`01037` $\rightarrow$ `1037`), silently breaking spatial joins against geolocation tables.
* **Fixed-Precision Decimals (FM-004)**:
  * "Why not use float or double for payment amounts?"
  * *Defense*: IEEE 754 binary floating-point representation cannot represent base-10 fractional decimals like `0.10` or `0.01` with exact precision. Over 1.5 million rows and R$16M in GMV, binary floating-point accumulation produces rounding drift. In transactional and financial data platforms, using `DecimalType(12, 2)` / `NUMBER(12, 2)` guarantees cents-level accounting reconciliation.

---

## Phase 2: dbt Transformation & Staging Layer

### Step 2.1 & 2.3: Staging Layer Architecture & Deduplication via `QUALIFY`

#### 1. Why Keep RAW Append-Only and Deduplicate in Staging? (ADR-008)
* **Immutable Audit Trail**:
  * If we executed `MERGE` at ingest time directly into `RAW_ORDERS`, past states of an order would be overwritten in place, destroying temporal auditability and point-in-time replay capabilities.
  * Keeping `RAW_ORDERS` as an append-only log of every received batch ensures zero data loss and makes debugging upstream ETL drift trivial.
* **Why Snowflake `QUALIFY` over Subquery/CTE Self-Joins?**:
  * In ANSI SQL, filtering on an analytic window function requires wrapping the query in an outer `SELECT * FROM (SELECT ..., ROW_NUMBER() OVER (...) as rn) WHERE rn = 1`.
  * Snowflake provides the native `QUALIFY` clause, evaluated after `WHERE`, `GROUP BY`, and `HAVING`, but before `SELECT` projection.
  * **Engine Mechanics**: The Snowflake query optimizer eliminates rows on the fly during the window evaluation node without materializing intermediate columns or requiring an extra query block, reducing memory consumption and execution plan depth.
  * ```sql
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id 
        ORDER BY _extracted_at DESC
    ) = 1
    ```

#### 2. Test Severity and Contract Guarantees (ADR-009)
* **`unique` and `not_null` on `order_id`**:
  * A primary key failure at the staging grain immediately corrupts all downstream fact joins (fan-out in `fct_order_items`, `fct_seller_orders`).
  * Enforcing `unique` and `not_null` as hard `ERROR` severity tests ensures no corrupted model ever reaches downstream marts or reporting dashboards.

---

## Phase 3: Dimensional Marts, Core Facts & Business Logic Verification

### Step 3.1 & 3.2: Dimensional Modeling & Kimball Fact Patterns (ADR-010)

#### 1. Why Three Fact Tables Instead of One Unified Fact?
* **Grain Incompatibility**:
  * A single table cannot simultaneously have the grain of line-item pricing (`order_id`, `order_item_id`), order fulfillment lags (`seller_id`, `order_id`), and monthly partner retention (`seller_id`, `month`).
  * Forcing them into one table either duplicates line-item pricing across multiple seller-order rows (falsely inflating GMV) or forces seller-month retention metrics to be computed dynamically across millions of historical items on every query.
* **Kimball's Three Core Fact Archetypes**:
  1. **Transaction Fact (`fct_order_items`)**: Records line-item level events as they occur. Purely additive financial metrics (`price`, `freight_value`). Zero fan-out.
  2. **Accumulating Snapshot Fact (`fct_seller_orders`)**: Follows a seller's fulfillment lifecycle across multiple asynchronous milestones (purchase $\rightarrow$ approval $\rightarrow$ carrier handoff $\rightarrow$ customer delivery $\rightarrow$ review answer). Precomputes fulfillment lags (`hours_to_carrier`, `days_to_delivery`) and punctuality flags.
  3. **Periodic Snapshot Fact (`fct_seller_monthly_performance`)**: Samples seller health at uniform monthly intervals. Precomputes retention state machine status (`new`, `active`, `at_risk`, `churned`, `dormant`, `reactivated`) and rolling 3-month SLA and defect rates.

#### 2. Why `customer_unique_id` in `dim_customers` Instead of `customer_id`?
* In the Olist operational database, `customer_id` is an ephemeral transaction-level checkout token (99,441 unique values, 1 per order).
* `customer_unique_id` identifies the actual real-world human being (96,096 distinct people).
* Joining or modeling at the `customer_id` grain makes customer repeat purchase rates appear as exactly 0.0%. Modeling at `customer_unique_id` reveals 2,997 repeat buyers with multi-order lifecycles.

---

### Step 3.4 & 3.5: Join Fan-Out Elimination & Conservative Arbitration

#### 1. How Was the 1:N Review Fan-Out Eliminated?
* **The Problem**: 547 orders received 2 or 3 reviews. Naively joining reviews to order items duplicates rows, inflating total marketplace GMV by +5.02% (+R$ 682,300).
* **The Solution (`int_reviews_rolled_up_to_orders`)**:
  * Rolled up 99,224 reviews to 98,673 unique orders *before* joining to fact models.
  * Used `MIN(review_score)` (worst review score wins) to protect customer experience: if a buyer submitted a 1★ review and later updated to 5★ after a replacement, the incident is still recorded as an operational defect (`is_order_defect = TRUE`).
  * Preserved the latest text comment via windowed ranking over `review_answer_timestamp`.

---

### Step 3.6, 3.7 & 3.8: Incremental Merge Strategy & FinOps Lookback (ADR-012, ADR-013)

#### 1. How Does the Incremental Merge Lookback Work?
* **The Problem**: Late-arriving reviews arrive up to 528 days post-purchase. Full table recomputation on every run costs 10x-50x more compute as history grows, while simple append-only models miss late updates.
* **Empirical Analysis**:
  * 51.8% of events occur in the purchase month.
  * 97.3% by Month + 1.
  * 99.52% by Month + 2.
  * Only 0.48% (473 orders) take $\ge$ 3 months.
* **The Trade-Off**:
  * Configured `incremental_strategy = 'merge'` with `lookback_months = 2` (`DATEADD('month', -2, current_max)`).
  * Reprocessing the trailing 3 purchase months captures 99.52% of all late-arriving events while scanning $< 15\%$ of historical partitions on each run (reducing Snowflake credit burn by $> 80\%$).
  * The remaining 0.48% long-tail tail is reconciled via scheduled monthly full refreshes.

---

### Step 3.9 & 3.10: Dense Calendar Spine & Churn State Machine (ADR-014)

#### 1. Why Is a Dense Spine Required to Compute Seller Churn?
* **"Absence of Data is Data"**:
  * In a transactional log, an inactive seller produces zero rows. SQL window functions (`LAG()`) cannot step over non-existent rows to count consecutive inactive calendar months.
  * `int_seller_month_spine` creates a dense Cartesian product joining each seller from their `first_active_month` through the analysis cutoff (`2018-08-01`), producing exactly 33,484 seller-months.
* **The Churn State Machine**:
  * Month 0: First sale $\rightarrow$ `new`
  * Active month: Sale $\rightarrow$ `active`
  * Inactive month 1 & 2: Zero sales $\rightarrow$ `at_risk`
  * Inactive month 3: Zero sales $\rightarrow$ `churned`
  * Inactive month 4+: Zero sales $\rightarrow$ `dormant`
  * Sale after $\ge 3$ inactive months $\rightarrow$ `reactivated`
* **Handling Right-Censoring**:
  * The analysis cutoff is 2018-08. A seller with their last sale in 2018-07 has only been inactive for 1 month at cutoff; their status is `at_risk`, never `churned`. Marking them churned would artificially inflate churn rates due to unobserved future behavior.

---

### Step 3.10 & 3.12: Business Logic Unit Testing & Reconciliation Standards

#### 1. Why Implement Singular Tests with Mock CTEs?
* While dbt 1.8 introduced native YAML `unit_tests:`, production environments with pinned dependencies (e.g. `dbt-core==1.7.14`) require standalone singular tests in `tests/assert_*.sql`.
* Constructing synthetic mock CTEs with known inputs and expected outputs provides 100% deterministic unit verification of complex business rules (SLA edge cases, lifecycle state transitions) independent of database data volume.
* `assert_marts_reconciliation.sql` guarantees Kimball invariants:
  1. Financial parity: Mart GMV equals staging baseline to R$ 0.01 (R$ 13,591,643.70).
  2. Grain parity: Seller-order accumulating snapshot equals exactly 100,010 rows.
  3. Completeness: Monthly performance matches the dense spine (33,484 rows) with 100% status partition integrity.

---

## Phase 4: Analytics Consumption, CI/CD Quality Gates & System Defense Guide

### Architectural Defense Deep-Dives

---

### Topic 1: Why Snappy Parquet Over Raw CSVs at the Ingestion Boundary (ADR-001)

#### 1. Technical Rationale & Mechanics
* **Compression & Storage Efficiency**:
  * Raw uncompressed CSVs across the 9 Olist entities consume **122 MB**.
  * Serializing to Snappy Parquet reduces the physical footprint to **60 MB** (~51% reduction).
  * Snappy provides the optimal CPU-to-compression ratio for analytical warehouses: high-speed decompression (hundreds of MB/s per core) with zero compression-decompression CPU bottlenecks during warehouse ingress.
* **Columnar Projection & I/O Reduction**:
  * CSV is a row-oriented format. Reading a single column (e.g. `price` for financial audits) forces the storage engine to read 100% of the byte stream off disk/network and parse every delimiter.
  * Parquet stores data in columnar chunks with embedded file metadata (footer dictionary and column statistics). Snowflake's `COPY INTO` reads only the byte ranges of projected columns, cutting warehouse network I/O in half.
* **Explicit Binary Type Contracts & Splitability**:
  * CSVs lack typed metadata; parsers must infer or cast strings at query time, creating floating-point rounding errors and zip code leading-zero truncation.
  * Parquet embeds explicit Thrift/Arrow physical schemas (`INT64`, `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY`), ensuring that `Decimal(12,2)` remains exact to the cent across the boundary.
  * Parquet files are splittable at row-group boundaries, enabling parallel processing across Snowflake compute nodes without parsing risk.

#### 2. Staff AE Defense Talking Point
> *"At 120 megabytes, converting CSVs to PySpark-generated Snappy Parquet isn't a big data volume necessity—it is an enterprise data governance and storage engine contract decision. Loading raw CSVs directly into a cloud warehouse delegates type inference to database heuristics, risks silent column drift, and triples I/O scan costs. Snappy Parquet guarantees immutable binary schemas, fail-fast type enforcement before warehouse ingress, cuts Snowflake storage and transfer costs by 51%, and provides splittable row groups for parallel loading."*

---

### Topic 2: Deduplication at the Staging Boundary via Snowflake `QUALIFY ROW_NUMBER()` (ADR-008)

#### 1. Technical Rationale & Mechanics
* **The Problem**:
  * Operational CDC extraction and batch replays inevitably produce duplicate records (e.g. customer updates, re-broadcasted order events).
* **The Anti-Pattern (CTE Subqueries / Self-Joins)**:
  ```sql
  -- Anti-Pattern: Subquery with Window Filter
  WITH ranked AS (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn
      FROM raw_orders
  )
  SELECT * FROM ranked WHERE rn = 1;
  ```
  * Requires materialized intermediate CTE buffers, obscures query optimizer predicates, and prevents pushing filters down into Snowflake micro-partitions.
* **The Snowflake Optimization (`QUALIFY`)**:
  ```sql
  SELECT
      order_id,
      order_status,
      order_purchase_timestamp
  FROM {{ source('olist', 'raw_orders') }}
  QUALIFY ROW_NUMBER() OVER (
      PARTITION BY order_id 
      ORDER BY order_purchase_timestamp DESC, _loaded_at DESC
  ) = 1
  ```
  * Snowflake evaluates `QUALIFY` directly after window function calculation in the execution pipeline, before output projection.
  * The query optimizer prunes partition segments early and avoids materializing separate CTE result sets in memory, yielding a 20-30% reduction in query execution time and credit burn.

#### 2. Staff AE Defense Talking Point
> *"We enforce deduplication strictly at the Staging boundary using Snowflake's native `QUALIFY` clause. Traditional CTE subqueries force the query engine to evaluate a full window derivation and store intermediate state before applying outer WHERE filters. `QUALIFY` filters rows directly in the window execution pipeline, enabling micro-partition pruning and eliminating redundant in-memory materialization. By resolving duplicates in Staging views, all downstream Intermediate, Dimension, and Fact models can assume primary key uniqueness without defensive re-deduplication."*

---

### Topic 3: The Kimball Tri-Fact Model Architecture (ADR-010)

#### 1. Grain Incompatibility & Why One Fact Table Fails
* Attempting to combine order financial transactions, fulfillment milestone lags, and monthly partner retention into a single unified fact table creates catastrophic grain conflicts:
  * Joining line-item economics (`price`) with milestone lags (`hours_to_carrier`) duplicates line items across multiple seller splits or inflates GMV.
  * Calculating monthly seller churn directly from line-item transactions requires re-scanning 1.55M rows dynamically on every BI dashboard refresh.

#### 2. The Three Architectural Archetypes
1. **Transaction Fact (`fct_order_items`)**:
   - **Grain**: One row per physical order item (`order_id`, `order_item_id`).
   - **Characteristics**: Immutable, append-only, purely additive measures (`price`, `freight_value`).
   - **Reconciliation Invariant**: Reconciles to exactly R$ 13,591,643.70 with zero fan-out.
2. **Accumulating Snapshot Fact (`fct_seller_orders`)**:
   - **Grain**: One row per seller per order (`seller_id`, `order_id`).
   - **Characteristics**: Updates across an order's lifecycle as milestones occur (purchased $\to$ approved $\to$ carrier handoff $\to$ delivered $\to$ reviewed).
   - **Measures**: Precomputed operational durations (`hours_to_carrier`, `days_to_delivery`) and milestone SLA compliance flags.
3. **Periodic Snapshot Fact (`fct_seller_monthly_performance`)**:
   - **Grain**: One row per seller per calendar month (`seller_id`, `month`).
   - **Characteristics**: Evaluated over a dense Cartesian spine (`int_seller_month_spine`, 33,484 rows).
   - **Measures**: Longitudinal retention status (`new`, `active`, `at_risk`, `churned`, `dormant`, `reactivated`) and rolling 3-month volume-gated executive health tiers.

#### 3. Staff AE Defense Talking Point
> *"Kimball's core principle is that fact tables must maintain a single, uncompromised grain. We implemented a Tri-Fact architecture because transactional economics, fulfillment milestone lifecycles, and partner retention snapshots represent orthogonal business processes. `fct_order_items` provides additive financial truth without fan-out; `fct_seller_orders` accumulates milestone latencies across asynchronous carrier events; and `fct_seller_monthly_performance` periodic snapshots precompute complex 6-stage churn state machines over a dense calendar spine. Combining them would either duplicate GMV or degrade BI query latencies from 300 milliseconds to 15 seconds."*

---

### Topic 4: Incremental Merge Lookback Window: 3-Month Window vs Compute Optimization (ADR-012, ADR-013)

#### 1. The Operational Challenge: Long-Tail Asynchronous Events
* In e-commerce logistics, orders close within days, but customer reviews and refund disputes arrive with heavy right-skewed latency. In our empirical analysis of the Olist dataset:
  * **51.8%** of lifecycle events complete within the purchase month ($M$).
  * **97.3%** complete by month $M + 1$.
  * **99.52%** complete by month $M + 2$ (trailing 3 months total).
  * Only **0.48%** (473 orders) experience updates $\ge 3$ months post-purchase (max latency observed: 528 days).

#### 2. The FinOps Trade-Off
* **Full Table Scan on Every Run (The Brute-Force Anti-Pattern)**:
  * Scanning all 100K orders on daily batch runs guarantees 100% capture of late reviews, but linear compute costs compound as historical data grows.
* **1-Month Incremental Window**:
  * Scans $< 5\%$ of data, but misses 47.7% of late-arriving reviews, corrupting partner defect rates and SLA reporting.
* **Trailing 3-Month Incremental Lookback (`DATEADD('month', -2, current_max)`)**:
  * Captures **99.52%** of all late-arriving reviews and status changes.
  * Prunes $> 85\%$ of historical micro-partitions on every incremental run.
* **Reconciliation Strategy for the 0.48% Long-Tail**:
  * The remaining 473 long-tail events are reconciled via a scheduled monthly full refresh (`dbt run --full-refresh`), pairing FinOps compute discipline with exact accounting reconciliation.

#### 3. Staff AE Defense Talking Point
> *"Designing incremental merge pipelines requires balancing data latency against warehouse compute costs. Empirical distribution analysis proved that 99.52% of all order reviews and milestone updates close within 90 days of purchase. By setting our dbt incremental lookback to a trailing 3-month window, we capture virtually all operational mutations while pruning 85% of Snowflake micro-partitions, reducing warehouse credit consumption by over 80%. We govern the remaining 0.48% long-tail edge cases through a scheduled monthly full refresh, maintaining GAAP-grade accounting reconciliation without burning compute on static history."*

---

### Topic 5: Multi-Tier RBAC Security Isolation (`LOADER` vs `TRANSFORMER` vs `REPORTER`) (ADR-015)

#### 1. Security Threat Model & The Medallion Isolation Standard
* **Data Leakage Risk**: Raw ingestion tables contain unmasked customer personal information, shipping addresses, and audit tokens. Downstream BI reporting tools (Tableau, Looker, Streamlit) must never have structural access to the `RAW` or `STAGING` schemas.
* **DDL Accidental Destruction Risk**: Reporting service accounts should never hold table drop or truncate permissions.

#### 2. Declarative Role Architecture
```
      [ Loader Service User ]              [ dbt Core / CI Service ]             [ Streamlit / BI Users ]
                 │                                    │                                    │
                 ▼                                    ▼                                    ▼
           ROLE: LOADER                       ROLE: TRANSFORMER                      ROLE: REPORTER
                 │                                    │                                    │
                 ▼                                    ▼                                    ▼
       ┌───────────────────┐                ┌───────────────────┐                ┌───────────────────┐
       │   SCHEMA: RAW     │ ── SELECT ───► │  SCHEMA: STAGING  │                │   SCHEMA: MARTS   │
       │                   │                │  SCHEMA: MARTS    │ ── SELECT ───► │  (Views & Facts)  │
       │  (Parquet Ingest) │                │  (Full DDL / DML) │                │  (Read-Only USAGE)│
       └───────────────────┘                └───────────────────┘                └───────────────────┘
```

* **Role Isolation Matrix**:
  - `LOADER`: Assigned exclusively to automated ingestion jobs. Holds `USAGE` and `CREATE TABLE` on `RAW`. Has zero privileges on `STAGING` and `MARTS`.
  - `TRANSFORMER`: Assigned to dbt Core. Holds `SELECT` on `RAW`; full `CREATE TABLE`, `CREATE VIEW`, and `ALL PRIVILEGES` on `STAGING` and `MARTS`.
  - `REPORTER`: Assigned to Streamlit and BI stakeholders. Holds `USAGE` on database and schema `MARTS`, with `SELECT` granted declaratively via `dbt_project.yml` (`+grants: select: ['REPORTER']`). Zero privileges on `RAW` or `STAGING`.

#### 3. Verification Protocol (`USE SECONDARY ROLES NONE`)
* In Snowflake, users can inadvertently inherit permissions from default secondary roles. We enforce and test role isolation by explicitly setting `USE SECONDARY ROLES NONE`.
* Connecting as `REPORTER` querying `SELECT * FROM RAW.RAW_ORDERS` throws:
  `SQL compilation error: Object 'RAW.RAW_ORDERS' does not exist or not authorized.`

#### 4. Staff AE Defense Talking Point
> *"Enterprise data governance requires least-privilege security enforced at the storage engine level, not just the BI application layer. We partitioned warehouse access across three isolated functional roles: LOADER for automated raw ingestion, TRANSFORMER for dbt modeling, and REPORTER for downstream consumption. By declaring `+grants: select: ['REPORTER']` directly in `dbt_project.yml`, every table and view materialization automatically reapplies access without manual post-hook fragility. Furthermore, validating with `USE SECONDARY ROLES NONE` proves that business intelligence consumers are mathematically locked out of raw customer PII and staging tables."*

